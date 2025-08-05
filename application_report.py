import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import pandahouse as ph

# Все запросы
# DAU - stickness
q_DAU = """
WITH dau AS 
(
  SELECT toStartOfDay(toDate(time)) AS day, count(DISTINCT user_id) AS users
    FROM simulator_20250520.feed_actions
   GROUP BY toStartOfDay(toDate(time))
  HAVING toStartOfDay(toDate(time)) BETWEEN today()-2 
     AND today()-1
), 
mau AS 
(
  SELECT toStartOfMonth(toDate(time)) AS month, count(DISTINCT user_id) AS users
    FROM simulator_20250520.feed_actions
   GROUP BY toStartOfMonth(toDate(time))
)
SELECT DISTINCT dau.day, dau.users, mau.users, dau.users/mau.users AS stickness
  FROM dau
 INNER JOIN simulator_20250520.feed_actions fd
    ON toStartOfDay(fd.time) = dau.day
 INNER JOIN mau
    ON toStartOfMonth(dau.day) = mau.month
 WHERE mau.users > 0
 ORDER BY fd.time
"""
# Анализ трафика 
feed_and_msg = """
SELECT toDate(time), uniqExact(user_id) as fm_users
  FROM (SELECT * FROM simulator_20250520.feed_actions ) fa
  JOIN (SELECT user_id FROM simulator_20250520.message_actions) ma
    ON fa.user_id = ma.user_id
 WHERE toDate(time) = today()-1
 GROUP BY toDate(time)
"""
only_feed = """
SELECT toDate(time), uniqExact(user_id) as f_users
  FROM simulator_20250520.feed_actions 
 WHERE user_id not in ( SELECT DISTINCT user_id
                          FROM simulator_20250520.message_actions
                       )
   AND toDate(time) = today()-1
 GROUP BY toDate(time)
"""

# Семидневный retention
q_retention_ads = """
SELECT start_day, day, source, count(user_id) AS users
  FROM 
    (SELECT user_id, min(toDate(time)) AS start_day
       FROM simulator_20250520.feed_actions
      GROUP BY user_id
     HAVING start_day BETWEEN today()-7
        AND today()-1) t1
      
       JOIN
       
    (SELECT DISTINCT user_id, toDate(time) AS day, source 
       FROM simulator_20250520.feed_actions) t2
      USING user_id 
      WHERE source = 'ads'
         
 GROUP BY start_day, day, source
"""

q_retention_organic = """
SELECT start_day, day, source, count(user_id) AS users
  FROM 
    (SELECT user_id, min(toDate(time)) AS start_day
       FROM simulator_20250520.feed_actions
      GROUP BY user_id
     HAVING start_day BETWEEN today()-7
        AND today()-1) t1
      
       JOIN
       
    (SELECT DISTINCT user_id, toDate(time) AS day, source 
       FROM simulator_20250520.feed_actions) t2
      USING user_id 
      WHERE source = 'organic'
         
 GROUP BY start_day, day, source"""
# Новые пользователя за 7 дней
q_new_users = """
SELECT first_day AS day,
       source,
       COUNT(user_id) AS new_users
FROM (
    SELECT user_id, source,
           toDate(MIN(time)) AS first_day
      FROM simulator_20250520.feed_actions
     GROUP BY user_id, source 
)
WHERE day BETWEEN today()-7
  AND today()-1
GROUP BY day, source 
"""
# Новые посты за 7 дней 
q_new_posts = """
SELECT first_day AS day,
        COUNT(1) AS new_posts
FROM (
    SELECT post_id,
           toDate(MIN(time)) AS first_day
      FROM simulator_20250520.feed_actions
     GROUP BY post_id
)
WHERE day BETWEEN today()-7
  AND today()-1
GROUP BY day
ORDER BY day
"""
my_token = '7244644521:AAHOiP8zKPaNYRkCnnd9uHPWNkZ-fYcis5g'
bot = telegram.Bot(token=my_token)
chat_id = -1002614297220

default_args = {
    'owner': 'v.makarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 30)
}

connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20250520',
'user':'student',
'password':'dpo_python_2020'
}

def make_app_report():
    
    # DAU-stickness с DoD
    DAU_stickness = ph.read_clickhouse(q_DAU, connection=connection)
    # Считаем DoD DAU 
    DAU_stickness['DoD_DAU%'] = ((
    DAU_stickness['dau.users'] - DAU_stickness['dau.users'].shift(1)) 
        / DAU_stickness['dau.users'].shift(1) * 100).round(2)
    DAU_stickness['DoD_DAU%'] = DAU_stickness['DoD_DAU%'].apply(
        lambda x: f"+{x:.2f}" if x > 0 else f"{x:.2f}")
    # Считаем DoD stickness
    DAU_stickness['DoD_stickness'] = ((
    DAU_stickness['stickness'] - DAU_stickness['stickness'].shift(1))).round(4)
    DAU_stickness['DoD_stickness'] = DAU_stickness['DoD_stickness'].apply(
        lambda x: f"+{x:.4f}" if x > 0 else f"{x:.4f}")
    # Первая таблица с динамикой DAU в приложении
    DAU_stickness.set_index("dau.day", inplace=True)
    max_index = DAU_stickness.index.max()
    DAU = DAU_stickness.loc[max_index, "dau.users"]
    stickness = round(DAU_stickness.loc[max_index, "stickness"],2)
    DoD_DAU = DAU_stickness.loc[max_index, "DoD_DAU%"]
    DoD_stickness = DAU_stickness.loc[max_index, "DoD_stickness"]
    
    # Анализ использования приложения
    feed_and_msg = ph.read_clickhouse(feed_and_msg, connection=connection)
    only_feed = ph.read_clickhouse(only_feed, connection=connection)
    info = feed_and_msg.merge(
    only_feed, on="toDate(time)")
    percent_usingfeedandmsg = (info.fm_users/(
        info.fm_users + info.f_users)).mul(100).round(0).to_numpy()[0]
    percent_usingonlyfeed = (info.f_users/(
        info.fm_users + info.f_users)).mul(100).round(0).to_numpy()[0]
    
    # Новые посты
    new_posts = ph.read_clickhouse(q_new_posts, connection=connection)
    new_posts['DoD%'] = ((
    new_posts['new_posts'] - new_posts['new_posts'].shift(1)) / new_posts['new_posts'].shift(1) * 100).round(2)
    new_posts['DoD%'] = new_posts['DoD%'].apply(
        lambda x: f"+{x:.2f}" if x > 0 else f"{x:.2f}")
    new_posts.set_index("day", inplace=True)

    posts = new_posts.loc[max_index, "new_posts"]
    DoD_posts = new_posts.loc[max_index, "DoD%"]
    
    # Новые пользователи по источнику
    new_users = ph.read_clickhouse(q_new_users, connection=connection)
    new_users.set_index('day', inplace=True)
    new_users.sort_values(by='day', inplace=True)
    new_users_sum = new_users.groupby("day").agg(new_users=("new_users","sum"))

    organic_new_users = new_users[new_users.source == 'organic']
    ads_new_users = new_users[new_users.source == 'ads']

    organic_new_users['DoD%'] = ((
    organic_new_users['new_users'] - organic_new_users['new_users'].shift(1))
        / organic_new_users['new_users'].shift(1) * 100).round(2)
    organic_new_users['DoD%'] = organic_new_users['DoD%'].apply(
        lambda x: f"+{x:.2f}" if x > 0 else f"{x:.2f}")

    ads_new_users['DoD%'] = ((
    ads_new_users['new_users'] - ads_new_users['new_users'].shift(1))
        / ads_new_users['new_users'].shift(1) * 100).round(2)
    ads_new_users['DoD%'] = ads_new_users['DoD%'].apply(
        lambda x: f"+{x:.2f}" if x > 0 else f"{x:.2f}")

    org = organic_new_users.loc[max_index, "new_users"]
    DoD_org = organic_new_users.loc[max_index, "DoD%"]

    ads = ads_new_users.loc[max_index, "new_users"]
    DoD_ads = ads_new_users.loc[max_index, "DoD%"]
    
    #Отправка сообщения
    msg=f"""
    Отчет на {max_index.date()}
    DAU: {DAU}({DoD_DAU}% DoD)
    stickness: {stickness}({DoD_stickness} DoD)
    Новые посты за день: {posts}({DoD_posts}% DoD)
    Новые органические пользователи: {org} ({DoD_org}% DoD)
    Новые рекламные пользователи: {ads} ({DoD_ads}%)
    {percent_usingfeedandmsg} % пользователей пользуются лентой и сообщениями
    {percent_usingonlyfeed} % пользователей пользуются только лентой"""
    
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    # Отправка изображения
    
    # Retention ads
    df_retention = ph.read_clickhouse(q_retention_ads, connection=connection)
    df_retention = df_retention.assign(
    cohort_period = (df_retention.day - df_retention.start_day).dt.days)
    cohort_pivot = df_retention.pivot_table(
    index='start_day', columns='cohort_period', values='users')
    cohort_size = cohort_pivot.iloc[:, 0]
    retention_matrix = cohort_pivot.divide(cohort_size, axis=0)
    ad_7day_ret = retention_matrix.iloc[0]

    # Retention organic
    df_retention = ph.read_clickhouse(q_retention_organic, connection=connection)
    df_retention = df_retention.assign(
    cohort_period = (df_retention.day - df_retention.start_day).dt.days)
    cohort_pivot = df_retention.pivot_table(
    index='start_day', columns='cohort_period', values='users')
    cohort_size = cohort_pivot.iloc[:, 0]
    retention_matrix = cohort_pivot.divide(cohort_size, axis=0)
    organic_7day_ret = retention_matrix.iloc[0]
    
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))

    sns.lineplot(data=ad_7day_ret, ax=axes[0,0])
    axes[0, 0].set_title('Retention рекламных пользователей')
    sns.lineplot(data=organic_7day_ret, ax=axes[0,1])
    axes[0, 1].set_title('Retention органических пользователей')

    sns.lineplot(data=new_users_sum, x='day', y='new_users', ax=axes[1,0])
    axes[1, 0].set_title('Новые пользователи')
    sns.lineplot(data=new_posts, x='day', y='new_posts', ax=axes[1,1])
    axes[1, 1].set_title('Новые посты')

    for ax in axes.flat:
        ax.tick_params(axis='x', rotation=45) 
    plt.tight_layout()

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'report.png'
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
@dag(default_args=default_args, catchup=False, schedule_interval='0 11 * * *')
def vladislav_makarov_bxs7496_app_report():
    @task()
    def make_report():
        make_app_report()
        
    make_report()
vladislav_makarov_bxs7496_app_report = vladislav_makarov_bxs7496_app_report()
    
    
    
    