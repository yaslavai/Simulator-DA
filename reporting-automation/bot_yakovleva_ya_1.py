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
    'start_date': datetime(2025, 6, 21),
}

schedule_interval = '0 8 * * *'

chat_id = '-1002614297220'
bot = telegram.Bot("7883825601:AAHE1TEl0xegzD4SeWl5AT_e2rGVbnl6N5k")

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def yakovleva_ya_dag_bot():
    
    # Выгрузка данных
    @task()
    def data_feeds():
        q = """
            SELECT toDate(time) AS date,
            uniqExact(user_id) AS DAU,
            likes/views AS ctr,
            countIf(action='view') AS views,
            countIf(action='like') AS likes
            FROM simulator_20250520.feed_actions
            WHERE toDate(time) BETWEEN yesterday() - 6 AND yesterday()
            GROUP BY date
            ORDER BY date
            """
        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    # Tекст с информацией о значениях ключевых метрик за предыдущий день
    @task()
    def previous_day(df):
        data = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
        yesterday = df[df['date'] == data].iloc[0]
        msg = (f"Данные за {data.strftime('%d.%m.%Y')}\n"f"DAU: {yesterday['DAU']}\n"f"Просмотры: {yesterday['views']}\n"f"Лайки: {yesterday['likes']}\n"f"CTR: {yesterday['ctr']:.6f}")
        bot.sendMessage(chat_id=chat_id, text=msg)
    
    # Графики с значениями метрик за предыдущие 7 дней
    @task()
    def charts(df):
        plt.figure(figsize=(10, 8))
        
        plt.subplot(2, 2, 1)
        sns.lineplot(data=df, x='date', y='DAU', color='red')
        plt.title('DAU')
        plt.ylabel('DAU')
        plt.xticks(rotation=45)

        plt.subplot(2, 2, 2)
        sns.lineplot(data=df, x='date', y='ctr', color='blue')
        plt.title('CTR')
        plt.ylabel('CTR')
        plt.xticks(rotation=45)

        plt.subplot(2, 2, 3)
        sns.lineplot(data=df, x='date', y='views', color='orange')
        plt.title('Просмотры')
        plt.ylabel('Просмотры')
        plt.xticks(rotation=45)

        plt.subplot(2, 2, 4)
        sns.lineplot(data=df, x='date', y='likes', color='green')
        plt.title('Лайки')
        plt.ylabel('Лайки')
        plt.xticks(rotation=45)

        plt.tight_layout()
        plot_object = io.BytesIO()
        plt.savefig(plot_object, format='png')
        plot_object.seek(0)
        plot_object.name = 'charts.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df_data = data_feeds()
    previous_day(df_data)
    charts(df_data) 
    
yakovleva_ya_dag_bot = yakovleva_ya_dag_bot()