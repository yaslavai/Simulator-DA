import pandahouse as ph
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import requests
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
db = 'simulator_20250520'

default_args = {
    'owner': 'yakovleva_ya',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 7, 4),
}

schedule_interval = '*/15 * * * *'

def check_anomaly_by_iqr(df, metric, a=5, n=5):
    # Алгоритм поиска аномалий в данных с помощью межквартильного размаха
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['low'] = df['low'].clip(lower=0) 
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if len(df.index) < 2:
        return 0, df

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    
    return is_alert, df

def check_anomaly_by_day_ago(df, metric, threshold=0.3):
    # Проверка значения на аномальность посредством
    # сравнения интересующего значения со значением в это же время сутки назад
    try:
        current_ts = df['ts'].max()
        day_ago_ts = current_ts - pd.DateOffset(days=1)

        current_value = df[df['ts'] == current_ts][metric].iloc[0]
        day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0]
        
        if day_ago_value == 0:
            return 0, current_value, 0 # Не можем сравнивать, если вчера был 0

        diff = abs(current_value / day_ago_value - 1)

        if diff > threshold:
            is_alert = 1
        else:
            is_alert = 0
        
        return is_alert, current_value, diff
    except Exception as e:
        # Если данных за вчера нет или другая ошибка - считаем, что аномалии нет
        print(f"Ошибка в check_anomaly_by_day_ago для метрики {metric}: {e}")
        return 0, 0, 0
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def yakovleva_ya_dag_alerts():
    
    @task
    def run_alerts():
        chat_id =  '-969316925'
        bot = telegram.Bot(token='7883825601:AAHE1TEl0xegzD4SeWl5AT_e2rGVbnl6N5k')

        query = f"""
        WITH feed_data AS (
            SELECT
                toStartOfFifteenMinutes(time) AS ts,
                formatDateTime(ts, '%R') AS hm,
                uniq(user_id) AS users_feed,
                countIf(user_id, action = 'view') AS views,
                countIf(user_id, action = 'like') AS likes,
                if(views > 0, likes / views, 0) AS ctr
            FROM {db}.feed_actions
            WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, hm
        ),
        msg_data AS (
            SELECT
                toStartOfFifteenMinutes(time) AS ts,
                formatDateTime(ts, '%R') AS hm,
                uniq(user_id) AS users_messenger,
                count(user_id) AS messages_sent
            FROM {db}.message_actions
            WHERE time >= today() - 1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, hm
        )
        SELECT
            feed_data.ts as ts,
            formatDateTime(feed_data.ts, '%d/%m %R') as date,
            users_feed,
            views,
            likes,
            ctr,
            users_messenger,
            messages_sent
        FROM feed_data
        FULL OUTER JOIN msg_data ON feed_data.ts = msg_data.ts
        ORDER BY ts
        """

        data = ph.read_clickhouse(query, connection=connection)
        data = data.fillna(0)
        int_metrics = ['users_feed', 'views', 'likes', 'users_messenger', 'messages_sent']
        data[int_metrics] = data[int_metrics].astype('int64')

        metrics_list = ['users_feed', 'views', 'likes', 'ctr', 'users_messenger', 'messages_sent']
        metrics_name = {'users_feed': 'Пользователи ленты', 
                        'views': 'Просмотры', 
                        'likes': 'Лайки', 
                        'ctr': 'CTR', 
                        'users_messenger': 'Пользователи мессенджера', 
                        'messages_sent': 'Отправленные сообщения'}

        for metric in metrics_list:
            df_metric = data[['date', 'ts', metric]].copy()

            # Вызываем оба метода проверки
            is_alert_iqr, df_metric_iqr = check_anomaly_by_iqr(df_metric, metric)
            is_alert_dod, current_val_dod, diff_dod = check_anomaly_by_day_ago(data, metric)

            # Отправляем алерт, если хотя бы 1 метод нашёл аномалию
            if is_alert_iqr == 1 or is_alert_dod == 1:

                readable_metric_name = metrics_name.get(metric, metric)
                # Расчет отклонения для IQR метода (для сообщения)
                current_val = df_metric_iqr[metric].iloc[-1]
                last_val = df_metric_iqr[metric].iloc[-2]
                if last_val > 0:
                    deviation = (current_val - last_val) / last_val * 100
                    deviation_str = f'{deviation:.2f}%'
                else:
                    deviation_str = 'N/A (предыдущее значение 0)'

                # Форматирование текущего значения
                if metric == 'ctr':
                    current_val_str = f'{current_val:.2%}'
                else:
                    current_val_str = f'{current_val:.0f}'

                msg = (f'🚨 Обнаружена аномалия в метрике:\n'
                       f'{readable_metric_name}\n'
                       f'Текущее значение: {current_val_str}\n'
                       f'Отклонение от недавних значений (IQR): {deviation_str}\n'
                       f'Отклонение от вчерашнего дня: {diff_dod:.2%}\n'
                       f'<a href="https://superset.lab.karpov.courses/superset/dashboard/7012/">Подробнее в дашборде 🔍</a>')

                sns.set(rc={'figure.figsize': (16, 10)})
                ax = sns.lineplot(x=df_metric_iqr['date'], y=df_metric_iqr[metric], label=f'Metric: {metric}')
                ax = sns.lineplot(x=df_metric_iqr['date'], y=df_metric_iqr['up'], label='Upper bound (IQR)')
                ax = sns.lineplot(x=df_metric_iqr['date'], y=df_metric_iqr['low'], label='Lower bound (IQR)')

                if metric == 'ctr':
                    current_ticks = ax.get_yticks()
                    new_labels = [f'{tick * 100:.0f}%' for tick in current_ticks]
                    ax.set_yticks(current_ticks)
                    ax.set_yticklabels(new_labels)

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 15 == 0: label.set_visible(True)
                    else: label.set_visible(False)

                plt.xticks(rotation=45, ha='right')
                ax.set(xlabel='Время', ylabel='Значение метрики', title=f'Подтвержденная аномалия в метрике: {metric}')
                ax.grid()

                plt.tight_layout()

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = f'{metric}_plot.png'
                plt.close()

                bot.sendPhoto(chat_id=chat_id, photo=plot_object, caption=msg, parse_mode='HTML')
        return
    
    run_alerts()
    
yakovleva_ya_dag_alerts = yakovleva_ya_dag_alerts()