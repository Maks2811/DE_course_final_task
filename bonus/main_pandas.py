from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
import subprocess
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


csv_file_path = '/opt/airflow/df/russian_houses.csv'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main_pandas',
    default_args=default_args,
    description='A  DAG for analyzing data, loading to clickhouse',
    schedule_interval=None,
)

""" функция загрузки данных в pandas, очистки, анализа и выгрузки в clickhouse"""
def query_pandas(**kwargs):
    try:
        df = pd.read_csv(csv_file_path, encoding='UTF-16LE')    # encoding='UTF-16LE'
        print(df.head())
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")

    print(f" Количество строк в датафрейме: {len(df)}")

    """Очистка  и преобразование типов данных"""
    df = df.fillna(0)

    def clean_year(year):
    # удаляем все символы не являющиеся цифрами
        if isinstance(year, str) and year.isdigit() and len(year)==4:
            return year
        else:
            return None

    def convert(value):
        if isinstance(value, str):
            value = value.replace(' ', '')  # удаляем пробелы
            value = value.replace(',', '.')  # заменяем запятые на точки
        
            return value

    pd.set_option('display.max_rows', 100)
    
    df['maintenance_year'] = df['maintenance_year'].apply(clean_year) 
    df['maintenance_year'] = df['maintenance_year'].fillna(0)
    df['maintenance_year'] = df['maintenance_year'].astype(int)
    df.loc[df['maintenance_year']>2024, 'maintenance_year']=0
    df_year_filtred = df[df['maintenance_year'] != 0]['maintenance_year']
    print(f'Средний год постройки зданий: {int(round(df_year_filtred.mean(), 0))}')
    print(f'Медианный год постройки зданий: {int(df_year_filtred.median())}')

    df['square'] = df['square'].apply(convert) 
    df['square']=df['square'].fillna(0)
    df.loc[df['square']=='—', 'square']=0 
    df['square'] = df['square'].astype(float)

    df.loc[df['population']=='—', 'population']=0 
    df['population'] = df['population'].apply(convert)
    df['population'] = df['population'].fillna(0)
    df['population'] = df['population'].astype(int)

    # Вычисляем и выводим на экран "Топ-10 регионов с наибольшим количеством объектов"
    df_region_top10 = df.groupby('region').count().sort_values(by='address', ascending=False).head(10)
    print(f"Топ-10 областей с наибольшим количеством объектов:\n {df_region_top10['address']}")


    # Сохраняем график "Топ-10 регионов с наибольшим количеством объектов" в файл
    plt.figure(figsize=(11,8))
    sns.barplot(data=df_region_top10 , x=df_region_top10.index , y='address')
    plt.xticks(rotation=90)
    plt.xlabel('Регион')
    plt.ylabel('Количество зданий')
    plt.title('Топ-10 регионов с наибольшим количеством объектов')
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/Top10_reg_obj_pandas.png')
    plt.show()

    # Вычисляем и выводим на экран "Топ-10 городов с наибольшим количеством объектов"
    # Москва это одновременно и город, и субъект федерации (регион)
    df_city_top10 = df[df['locality_name']!=0].groupby('locality_name').count().sort_values(by='address', ascending=False).head(10)
    print(f"Топ-10 городов с наибольшим количеством объектов:\n {df_city_top10['address']}")

    # Сохраняем график "Топ-10 городов с наибольшим количеством объектов" в файл
    plt.figure(figsize=(11, 8))
    sns.barplot(data=df_city_top10, x=df_city_top10.index, y='address')
    plt.xticks(rotation=90)
    plt.xlabel('Город')
    plt.ylabel('Количество зданий')
    plt.title('Топ-10 городов с наибольшим количеством объектов')
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/Top10_city_obj_pandas.png')
    plt.show()

    # Вычисляем и выводим здания с максимальной площадью в рамках каждого региона
    df_max_square = df[df['region'] != 0].groupby('region').max('square').sort_values(by='square', ascending=False)
    print(f"Максимальная площадь здания по регионам ( Топ-100): {df_max_square['square'].head(100)}")


    # Сохраняем график ТОП-20 максимальных площадей зданий по регионам
    plt.figure(figsize=(11, 8))
    sns.barplot(data=df_max_square.sort_values(by='square', ascending=False).head(20),
                x=df_max_square.sort_values(by='square', ascending=False).head(20).index, y='square')
    plt.xticks(rotation=90)
    plt.xlabel('Регион')
    plt.ylabel('Максимальная площадь здания')
    plt.title('Максимальная площадь здания по регионам')
    plt.ticklabel_format(style='plain', axis='y')  # Отключение научного формата на оси Y
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/max_square_pandas.png')
    plt.show()

    # Вычисляем и выводим здания с минимальной площадью в рамках каждого региона
    df_min_square = df[(df['region']!=0) & (df['square']!=0)].groupby('region').min('square').sort_values(by='square')
    print(f"Минимальная площадь здания по регионам ( Топ-100): {df_min_square['square'].head(100)}")

    # Сохраняем график ТОП-20 минимальных площадей зданий по регионам
    plt.figure(figsize=(11, 8))
    sns.barplot(data=df_min_square.sort_values(by='square').head(20),
                x=df_min_square.sort_values(by='square').head(20).index, y='square')
    plt.xticks(rotation=90)
    plt.xlabel('Регион')
    plt.ylabel('Минимальная площадь объектов')
    plt.title('Минимальная площадь объектов по регионам')
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/min_square_pandas.png')
    plt.show()

    # Подсчет и вывод распределения количества построенных зданий по десятилетиям
    df['decade'] = (df['maintenance_year'] // 10) * 10 # создаем новый столбец в датафрейме с декадами (десятилетиями)
    df_decade = df[df['decade']!=0].groupby('decade').count()[ 'maintenance_year'].sort_values(ascending=False)
    df_decade = df_decade.reset_index()
    df_decade = df_decade.rename(columns={'decade': 'Десятилетие', 'maintenance_year': 'Количество_зданий'})
    print(f"Распределение количества построенных зданий по десятилетиям: {df_decade}")


    # Сохраняем график ТОП-20 распределения количества построенных зданий по десятилетиям
    plt.figure(figsize=(11, 7))
    sns.barplot(data=df_decade.head(20),
                x=df_decade['Десятилетие'].head(20),
                y=df_decade['Количество_зданий'].head(20))
    plt.xticks(rotation=90)
    plt.xlabel('Десятилетие')
    plt.ylabel('Количество зданий')
    plt.title('Количество зданий по десятилетиям')
    plt.savefig('/opt/airflow/df/graphics/decade_pandas.png')
    plt.show()

    df = df.drop('decade', axis=1)

    # заменяем нули в столбцах со строковым типом данных  на '-' перед выгрузкой в clickhouse
    df[['region', 'locality_name', 'address', 'full_address', 'description']] = df[
        ['region', 'locality_name', 'address', 'full_address', 'description']].replace(0, '-')
    
    # подключаемся к clickhouse
    client = Client('clickhouse_user')

    # Преобразовываем DataFrame в список кортежей
    data_tuples = [tuple(x) for x in df.to_numpy()]

    # Загружаем данные в ClickHouse
    client.execute('INSERT INTO houses_db.table_pd VALUES', data_tuples)

""" функция создания БД и таблицы в clickhouse"""
def create_clickhouse_table(**kwargs):
    # Используем HttpHook для отправки запроса в ClickHouse
    clickhouse_hook = HttpHook(http_conn_id='clickhouse_default', method='POST')

    # Создание базы данных houses_db, если она не существует
    create_database_query = "CREATE DATABASE IF NOT EXISTS houses_db"
    headers = {'Content-Type': 'application/sql'}
    response_db = clickhouse_hook.run(endpoint='', data=create_database_query, headers=headers)
    if response_db.status_code == 200:
        print("База данных создана успешно или уже существует.")
    else:
        print("Ошибка при создании БД:", response_db.text)
        return

    # В случае если таблица в БД существует - удаляем ее, чтобы пересоздать новую.
    # это делается для того, чтобы при  очередной заливке данных из датафрейма не было старых или дублирующихся данных
    drop_table_query = "DROP TABLE IF EXISTS houses_db.table_pd"
    headers = {'Content-Type': 'application/sql'}
    response = clickhouse_hook.run(endpoint='', data=drop_table_query, headers=headers)
    if response.status_code == 200:
        print("Старая таблица  успешно удалена или таблицы не существовало .")
    else:
        print("Ошибка при удалении таблицы:", response.text)

    # Создание новой таблицы в базе данных houses_db
    create_table_query = """
    CREATE TABLE IF NOT EXISTS houses_db.table_pd
    (        
        house_id Int64,
        latitude Float64,
        longitude Float64,
        maintenance_year Int32,
        square Float64, 
        population Int32,
        region String,
        locality_name String,
        address String,
        full_address String,
        communal_service_id Float64,
        description String      
        
    ) ENGINE = MergeTree()
    ORDER BY house_id
    """
    headers = {'Content-Type': 'application/sql'}
    response = clickhouse_hook.run(endpoint='', data=create_table_query, headers=headers)
    if response.status_code == 200:
        print("Таблица  создана успешно или уже существует .")
    else:
        print("Ошибка при создании таблицы:", response.text)


"""функция отправки SQL запроса в clickhouse"""
def sql_query_clickhouse(**kwargs):
    # подключаемся к clickhouse
    client = Client('clickhouse_user')
    # выполняем запрос "Топ 25 домов, у которых площадь больше 60 кв.м"
    result = client.execute('SELECT square , full_address FROM houses_db.table_pd WHERE square > 60.0 ORDER BY square DESC LIMIT(25)')
    print(result)





create_table_task = PythonOperator(
        task_id='create_clickhouse_table',
        python_callable=create_clickhouse_table,
        dag=dag,
    )

task_query_pandas = PythonOperator(
    task_id='query_pandas',
    python_callable=query_pandas,
    dag=dag,

    )

clickhouse_query_task = PythonOperator(
        task_id='sql_query_clickhouse',
        python_callable=sql_query_clickhouse,
        dag=dag,
    )



create_table_task >> task_query_pandas >> clickhouse_query_task
