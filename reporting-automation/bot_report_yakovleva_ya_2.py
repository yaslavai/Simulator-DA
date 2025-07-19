import pandahouse as ph
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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

default_args = {
    'owner': 'yakovleva_ya',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 25),
}

schedule_interval = '0 8 * * *'

chat_id = '-1002614297220'
bot = telegram.Bot("7883825601:AAHE1TEl0xegzD4SeWl5AT_e2rGVbnl6N5k")

FEED_TABLE = 'simulator_20250520.feed_actions'
MESSAGE_TABLE = 'simulator_20250520.message_actions'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['reports', 'telegram'])
def yakovleva_ya_report_bot():
    
    @task(task_id='get_data_from_clickhouse')
    def get_data():

        context = get_current_context()
        # Логическая дата запуска
        ds = context['ds'] 

        query_template = f"""
            WITH feed_metrics AS (
                SELECT toDate(time) as event_date, uniq(user_id) as feed_dau, countIf(action, action='view') as views, countIf(action, action='like') as likes
                FROM {FEED_TABLE} GROUP BY event_date
            ),
            message_metrics AS (
                SELECT toDate(time) as event_date, uniq(user_id) as messenger_dau, count() as messages_sent
                FROM {MESSAGE_TABLE} GROUP BY event_date
            ),
            dau_metrics AS (
                SELECT toDate(time) as event_date, uniq(user_id) as dau
                FROM (SELECT user_id, time FROM {FEED_TABLE} UNION ALL SELECT user_id, time FROM {MESSAGE_TABLE})
                GROUP BY event_date
            )
            SELECT 
                d.event_date AS event_date, 
                d.dau AS dau, 
                f.feed_dau AS feed_dau, 
                f.views AS views, 
                f.likes AS likes,
                m.messenger_dau AS messenger_dau, 
                m.messages_sent AS messages_sent
            FROM dau_metrics d
            LEFT JOIN feed_metrics f ON d.event_date = f.event_date
            LEFT JOIN message_metrics m ON d.event_date = m.event_date
            {{where_clause}}
        """

        # Строго за вчерашний день
        q_yesterday_report = query_template.format(where_clause=f"WHERE d.event_date = toDate('{ds}')")
        yesterday_df = ph.read_clickhouse(q_yesterday_report, connection=connection)

        # За последние 30 дней
        q_monthly_report = query_template.format(where_clause=f"WHERE d.event_date BETWEEN toDate('{ds}') - 29 AND toDate('{ds}') ORDER BY d.event_date")
        monthly_df = ph.read_clickhouse(q_monthly_report, connection=connection)

        # Запросы для распределений (за весь период)
        q_source_dist = f"SELECT source, uniq(user_id) as unique_users FROM (SELECT user_id, source FROM {FEED_TABLE} UNION ALL SELECT user_id, source FROM {MESSAGE_TABLE}) GROUP BY source"
        source_dist = ph.read_clickhouse(q_source_dist, connection=connection)
        q_os_dist = f"SELECT os, uniq(user_id) as unique_users FROM (SELECT user_id, os FROM {MESSAGE_TABLE} UNION ALL SELECT user_id, os FROM {FEED_TABLE}) GROUP BY os"
        os_dist = ph.read_clickhouse(q_os_dist, connection=connection)
        q_total_users = f"SELECT uniq(user_id) as total_users FROM (SELECT user_id FROM {FEED_TABLE} UNION ALL SELECT user_id FROM {MESSAGE_TABLE})"
        total_unique_users = ph.read_clickhouse(q_total_users, connection=connection)['total_users'][0]
        
        return {
            'yesterday_df': yesterday_df.to_json(),
            'monthly_df': monthly_df.to_json(),
            'source_dist': source_dist.to_json(),
            'os_dist': os_dist.to_json(),
            'total_unique_users': total_unique_users
        }

    @task(task_id='prepare_and_send_report')
    def send_report(data: dict):

        yesterday_df = pd.read_json(data['yesterday_df'])
        monthly_df = pd.read_json(data['monthly_df']) 
        source_dist = pd.read_json(data['source_dist'])
        os_dist = pd.read_json(data['os_dist'])
        total_unique_users = data['total_unique_users']

        # Универсальная функция для обработки данных
        def process_df(df):
            df['event_date'] = pd.to_datetime(df['event_date'], unit='ms')
            df = df.fillna(0)
            numeric_cols = ['dau', 'feed_dau', 'views', 'likes', 'messenger_dau', 'messages_sent']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            df['ctr'] = (df['likes'] / df['views'].replace(0, 1) * 100)
            df['msg_per_user'] = (df['messages_sent'] / df['messenger_dau'].replace(0, 1))
            return df

        yesterday_df = process_df(yesterday_df)
        monthly_df = process_df(monthly_df)
        
        # Формирование отчета по данным за вчера
        last_day_data = yesterday_df.iloc[0]
        last_date_str = last_day_data['event_date'].strftime('%d.%m.%Y')
        report_text = f"""
*📊 Отчет по приложению за {last_date_str}*

*📈 Общие метрики*
- *DAU (общее):* {last_day_data['dau']:,}
- *Всего уникальных пользователей (за весь период):* {total_unique_users:,}

*📰 Лента новостей*
- *Пользователи ленты (Feed DAU):* {int(last_day_data['feed_dau']):,}
- *Просмотры:* {int(last_day_data['views']):,}
- *Лайки:* {int(last_day_data['likes']):,}
- *CTR:* {last_day_data['ctr']:.2f}%

*💬 Мессенджер*
- *Пользователи мессенджера (Msg DAU):* {int(last_day_data['messenger_dau']):,}
- *Отправлено сообщений:* {int(last_day_data['messages_sent']):,}
- *Сообщений на пользователя:* {last_day_data['msg_per_user']:.2f}
        """
        bot.send_message(chat_id, report_text, parse_mode='Markdown')

        sns.set_style("whitegrid")
        fig, axes = plt.subplots(2, 2, figsize=(18, 10))
        fig.suptitle(f'Ключевые метрики за последние 30 дней (до {last_date_str})', fontsize=20)

        # График DAU
        sns.lineplot(ax=axes[0, 0], data=monthly_df, x='event_date', y='dau', marker='o')
        axes[0, 0].set_title('Динамика DAU по приложению', fontsize=16)
        axes[0, 0].set_xlabel('Дата') 
        axes[0, 0].set_ylabel('Кол-во пользователей') 

        # График CTR
        sns.lineplot(ax=axes[0, 1], data=monthly_df, x='event_date', y='ctr', marker='o', color='green')
        axes[0, 1].set_title('Динамика CTR в ленте', fontsize=16)
        axes[0, 1].set_xlabel('Дата') 
        axes[0, 1].set_ylabel('CTR, %') 

        # График вовлеченности
        sns.lineplot(ax=axes[1, 0], data=monthly_df, x='event_date', y='msg_per_user', marker='o', color='purple')
        axes[1, 0].set_title('Динамика вовлеченности в мессенджере', fontsize=16)
        axes[1, 0].set_xlabel('Дата') 
        axes[1, 0].set_ylabel('Сообщений на пользователя') 
        
        # Структура аудитории
        source_dist['category'] = 'Source'
        os_dist['category'] = 'OS'
        combined_dist = pd.concat([source_dist.rename(columns={'source': 'label'}), os_dist.rename(columns={'os': 'label'})])
        sns.barplot(ax=axes[1, 1], data=combined_dist, y='label', x='unique_users', hue='category', dodge=False)
        axes[1, 1].set_title('Структура аудитории', fontsize=16)
        axes[1, 1].set_xlabel('Кол-во уникальных пользователей') 
        axes[1, 1].set_ylabel('') 

        # Оптимизация подписей и отправка
        for ax in [axes[0,0], axes[0,1], axes[1,0]]:
            ax.tick_params(axis='x', rotation=45)
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        bot.send_photo(chat_id, photo=buf, caption='Аналитический дашборд по приложению')

    report_data = get_data()
    send_report(report_data)

yakovleva_ya_report_bot = yakovleva_ya_report_bot()