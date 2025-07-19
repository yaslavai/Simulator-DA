import pandahouse as ph
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Подключаемся
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20250520',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }


default_args = {
    'owner': 'yakovleva_ya',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 16),
}


schedule_interval = '0 7 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def yakovleva_ya_dag():
    
    # Новости
    @task()
    def feeds():
        q_feeds = '''
                    SELECT toDate(time) as event_date, gender, age, os, user_id, 
                    sum(action = 'like') as likes, sum(action = 'view') as views
                    FROM simulator_20250520.feed_actions
                    WHERE toDate(time)  = yesterday()
                    GROUP BY toDate(time), gender, age, os, user_id
                    '''
        df_fa = ph.read_clickhouse(query=q_feeds, connection=connection)
        return df_fa

    df_fa = feeds()

    # Сообщения
    @task()
    def messages():
        q_messages = '''
                        SELECT yesterday() as event_date,
                        COALESCE(t1.user_id, t2.receiver_id) as user_id,
                        messages_received, messages_sent, users_received, users_sent
                        FROM    (SELECT user_id, toDate(time) as date,
                                count(user_id) as messages_sent,
                                uniq(receiver_id) as users_sent
                                FROM simulator_20250520.message_actions
                                WHERE date = yesterday()
                                GROUP BY user_id, date) t1

                                FULL OUTER JOIN

                                (SELECT receiver_id, toDate(time) as date,
                                count(receiver_id) as messages_received,
                                uniq(user_id) as users_received
                                FROM simulator_20250520.message_actions
                                WHERE date = yesterday()
                                GROUP BY receiver_id, date) t2
                                ON t1.user_id = t2.receiver_id
                                '''
        df_ma = ph.read_clickhouse(query=q_messages, connection=connection)
        return df_ma
    
    df_ma = messages() 


    @task
    def merge_df(df_fa, df_ma):

        q_f = '''
                SELECT DISTINCT user_id, os, gender, age
                FROM simulator_20250520.feed_actions
                '''
        
        q_m = '''
                SELECT DISTINCT user_id, os, gender, age
                FROM simulator_20250520.message_actions
                '''
        
        # Мёрджим
        df_f = ph.read_clickhouse(q_f, connection=connection)
        df_m = ph.read_clickhouse(q_m, connection=connection)
        df_merged = pd.merge(df_f, df_m, on='user_id', how='outer').drop_duplicates('user_id')
        df_f_m = df_fa.merge(df_ma, on=['event_date', 'user_id'], how='outer')
        df = df_f_m.merge(df_merged, on='user_id', how='left')
        return df
    
    df = merge_df(df_fa, df_ma)

    # Преобразование
    @task
    def transform(df):
        df['gender'] = df['gender_x'].fillna(df['gender_x']).fillna(df['gender_y'])
        df['age'] = df['age_x'].fillna(df['age_x']).fillna(df['age_y'])
        df['os'] = df['os_x'].fillna(df['os_x']).fillna(df['os_y'])
        df['likes'] = df['likes'].fillna(0)
        df['views'] = df['views'].fillna(0)
        df['messages_received'] = df['messages_received'].fillna(0)
        df['messages_sent'] = df['messages_sent'].fillna(0)
        df['users_received'] = df['users_received'].fillna(0)
        df['users_sent'] = df['users_sent'].fillna(0)
        df.drop(['os_x', 'gender_x', 'age_x', 'os_y', 'gender_y', 'age_y'], axis=1, inplace=True)
        return df

    result = transform(df)


    # os
    @task()
    def os(result):
        df_os = result[['event_date',
                        'os',
                        'views',
                        'likes',
                        'messages_received',
                        'messages_sent',
                        'users_received',
                        'users_sent']].groupby(['event_date', 'os'], as_index=False).sum().rename(columns={'os':'dimension_value'})
        dimension_col = 'os'
        df_os.insert(1, 'dimension', dimension_col)
        return df_os
    
    # gender 
    @task()
    def gender(result):
        df_gender = result[['event_date',
                           'gender',
                           'views',
                           'likes',
                           'messages_received',
                           'messages_sent',
                           'users_received',
                           'users_sent']].groupby(['event_date', 'gender'], as_index=False).sum().rename(columns={'gender':'dimension_value'})
        dimension_col = 'gender'
        df_gender.insert(1, 'dimension', dimension_col)
        return df_gender

    # age
    @task()
    def age(result):
        df_age = result[['event_date',
                        'age',
                        'views',
                        'likes',
                        'messages_received',
                        'messages_sent',
                        'users_received',
                        'users_sent']].groupby(['event_date', 'age'], as_index=False).sum().rename(columns={'age':'dimension_value'})
        dimension_col = 'age'
        df_age.insert(1, 'dimension', dimension_col)
        return df_age
    
    df_os = os(result)
    df_gender = gender(result)
    df_age = age(result) 

    
    # Загружаем данные
    @task()
    def load(df_os, df_gender, df_age):
        res = pd.concat([df_os, df_gender, df_age])
        res = res.astype({
            'views': 'int',
            'likes': 'int',
            'messages_received': 'int',
            'messages_sent': 'int',
            'users_received': 'int',
            'users_sent': 'int'})
        
        table = '''
                CREATE TABLE IF NOT EXISTS test.yakovleva_ya_sda
                        (event_date Date,
                        dimension String,
                        dimension_value String,
                        views Int64, 
                        likes Int64,  
                        messages_received Int64,     
                        messages_sent Int64,     
                        users_received Int64,    
                        users_sent Int64)
                        ENGINE = MergeTree()
                        ORDER BY event_date'''
        ph.execute(table, connection=connection_test)
        ph.to_clickhouse(df=res, table='yakovleva_ya_sda', index=False, connection=connection_test)

    load(df_os, df_gender, df_age)

yakovleva_ya_dag = yakovleva_ya_dag()