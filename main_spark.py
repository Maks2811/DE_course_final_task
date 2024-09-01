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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg,  when, max, min, floor
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

csv_file_path = '/opt/airflow/df/russian_houses.csv'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main_spark',
    default_args=default_args,
    description='A  DAG for analyzing data, loading to clickhouse',
    schedule_interval=None,
)


""" Функция загрузки данных, их очистки, анализа, построения графиков и выгрзуи в БД clickhouse """
def query_pyspark(**kwargs):

    # Инициализация SparkSession
    spark = SparkSession.builder \
        .appName("Create DataFrame from CSV") \
        .master("local[*]") \
        .getOrCreate()
    df =spark.read.csv(csv_file_path, header=True, inferSchema=True, encoding='UTF-16LE', multiLine=True)
    df.show()
    print('\nНачальная структура датафрейма после загрузки данных в него:')
    df.printSchema()

    row_count = df.count()
    print(f"\nКоличество строк в датафрейме: {row_count}")

    # Определяем функцию очистки даты постройки
    def clean_year(year):
        if isinstance(year, str) and year.isdigit() and len(year) == 4:
            return year
        else:
            return None

    # Определяем функцию удаления пробелов и замены запятых на точки
    def convert(value):
        if isinstance(value, str):
            value = value.replace(' ', '')  # удаляем пробелы
            value = value.replace(',', '.')  # заменяем запятые на точки
            return value

    # Регистрируем UDF для функции clean_year
    clean_year_udf = udf(clean_year, StringType())

    df = df.withColumn("maintenance_year", clean_year_udf(df["maintenance_year"]))

    # Заменяем значения NULL на нули в числовых и строковых колонках
    df = df.fillna(0)
    df = df.fillna('0')

    # Проверяем не остались ли у нас занчения NULL в датафрейме
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    print("\nКоличество значений NULL после их заполнения нулями:")
    null_counts.show()
    
    # преобразуем данные в колонке maintenance_year в тип int
    df = df.withColumn("maintenance_year", col("maintenance_year").cast("int"))

    # заменяем все выбросы в maintenance_year (года больше 2024) на 0
    df = df.withColumn("maintenance_year", when(col("maintenance_year") > 2024, 0).otherwise(col("maintenance_year")))

    # Регистрируем UDF для функции convert
    convert_udf = udf(convert, StringType())
    df = df.withColumn("square", convert_udf(df["square"]))

    df = df.withColumn("square", convert_udf(df["square"]))  # убираем пробелы и запятые из колонки square
    df = df.withColumn("square", when(col("square") == "—", "0").otherwise(
        col("square")))  # заменяем все '—' на '0' в колонке square

    # преобразуем данные в колонке square в тип float
    df = df.withColumn("square", col("square").cast("float"))

    df = df.withColumn("population", convert_udf(df["population"]))  # убираем пробелы и запятые из колонки population
    df = df.withColumn("population", when(col("population") == "—", "0").otherwise(
        col("population")))  # Заменяем все '—' на '0' в колонке population

    # преобразуем данные в колонке population в тип int
    df = df.withColumn("population", col("population").cast("int"))

    # преобразуем данные в колонке communal_service_id в тип float
    df = df.withColumn("communal_service_id", col("communal_service_id").cast("float"))

    # Повторно заменяем нулями значения NULL которые могли появиться в ходе преобразований данных
    df = df.fillna(0)

    print('\nИтоговая структура датафрейма после всех преобразований')
    df.printSchema()

    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
    print("\nИтогое количество значений NULL после всех преобразований:")
    null_counts.show()

    year_avg = round(df.filter(df["maintenance_year"] != 0).select(avg("maintenance_year")).collect()[0][0])
    print(f'\nСредний год постройки зданий: {year_avg}')

    year_median = df.filter(df["maintenance_year"] != 0).approxQuantile("maintenance_year", [0.5], 0.01)[0]
    print(f'\nМедианный год постройки зданий: {year_median}')


    # Определяем топ-10 областей  с наибольшим количеством объектов.
    df_region_top = df.groupBy("region").count()
    df_region_top = df_region_top.orderBy(col("count").desc())
    print("\nТоп-10 областей с наибольшим количеством объектов")
    df_region_top.limit(10).show()

    df_region_top10_pd = df_region_top.limit(10).toPandas()  # преобразуем датафрейм в pandas для отрисовки графика

    # Сохраняем график топ-10 областей  с наибольшим количеством объектов в файл
    plt.figure(figsize=(11, 8))
    sns.barplot(x='region', y='count', data=df_region_top10_pd)
    plt.xticks(rotation=90)
    plt.xlabel('Регион')
    plt.ylabel('Количество зданий')
    plt.title('Топ-10 областей  с наибольшим количеством объектов')
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/Top10_reg_obj_pySpark.png')
    plt.show()

    # Определяем топ-10 городов  с наибольшим количеством объектов.
    df_city_top = df.filter(df["locality_name"] != '0').groupBy("locality_name").count()
    df_city_top = df_city_top.orderBy(col("count").desc())
    print("\nТоп-10 городов с наибольшим количеством объектов")
    df_city_top.limit(10).show()

    df_city_top10_pd = df_city_top.limit(10).toPandas()  # преобразуем датафрейм в pandas для отрисовки графика

    # Сохраняем график топ-10 городов  с наибольшим количеством объектов в файл
    plt.figure(figsize=(11, 8))
    sns.barplot(x='locality_name', y='count', data=df_city_top10_pd)
    plt.xticks(rotation=90)
    plt.xlabel('Город')
    plt.ylabel('Количество зданий')
    plt.title('Топ-10 городов  с наибольшим количеством объектов')
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/Top10_city_obj_pySpark.png')
    plt.show()

    # Находим здания с максимальной  площадью в рамках каждой области.
    df_max_square = df.filter(df["region"] != '0').groupBy("region").agg(max("square").alias("max_square"))
    df_max_square = df_max_square.orderBy(col("max_square").desc())
    print('\nЗдания с максимальной  площадью в рамках каждой области')
    df_max_square.show()

    df_max_square_pd = df_max_square.limit(20).toPandas()  # преобразуем датафрейм в pandas для отрисовки графика

    # Сохраняем график "Здания с максимальной  площадью в рамках каждой области (Топ-20)"  в файл
    plt.figure(figsize=(11, 8))
    sns.barplot(data=df_max_square_pd, x="region", y="max_square")
    plt.xticks(rotation=90)
    plt.xlabel('Регион')
    plt.ylabel('Максимальная площадь здания')
    plt.title('Максимальная площадь здания по регионам')
    plt.ticklabel_format(style='plain', axis='y')  # Отключение научного формата на оси Y
    plt.show()
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/max_square_pySpark.png')

    # Находим здания с минимальной  площадью в рамках каждой области.
    df_min_square = df.filter((df["region"] != '0') & (df["square"] != '0')).groupBy("region").agg(
        min("square").alias("min_square"))
    df_min_square = df_min_square.orderBy(col("min_square"))
    print('\nЗдания с минимальной  площадью в рамках каждой области')
    df_min_square.show()

    df_min_square_pd = df_min_square.limit(20).toPandas()  # преобразуем датафрейм в pandas для отрисовки графика

    # Сохраняем график "Здания с минимальной площадью в рамках каждой области (Топ-20)"  в файл
    plt.figure(figsize=(11, 8))
    sns.barplot(data=df_min_square_pd, x="region", y="min_square")
    plt.xticks(rotation=90)
    plt.xlabel('Регион')
    plt.ylabel('Минимальная площадь здания')
    plt.title('Минимальная площадь здания по регионам')
    plt.ticklabel_format(style='plain', axis='y')  # Отключение научного формата на оси Y
    plt.show()
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/min_square_pySpark.png')

    # Добавляем в датафрейм колонку с десятилетиями
    df = df.withColumn("decade", (floor(col("maintenance_year") / 10) * 10))

    # Определяем количество зданий по десятилетиям
    df_decade = df.filter(df["decade"] != '0').groupBy("decade").count()
    df_decade = df_decade.withColumnRenamed("count", "Количество_зданий")
    df_decade = df_decade.withColumnRenamed("decade", "Десятилетие")
    df_decade = df_decade.orderBy(col("Количество_зданий").desc())

    print('\nКоличество зданий по десятилетиям')
    df_decade.show()

    df_decade_pd = df_decade.limit(20).toPandas()     # преобразуем датафрейм в pandas для отрисовки графика

    # Сохраняем график "Количество зданий по десятилетиям"  в файл
    plt.figure(figsize=(11, 8))
    sns.barplot(data=df_decade_pd, x='Десятилетие', y='Количество_зданий')
    plt.xticks(rotation=90)
    plt.xlabel('Десятилетие')
    plt.ylabel('Количество зданий')
    plt.title('Количество зданий по десятилетиям')
    plt.tight_layout()
    plt.savefig('/opt/airflow/df/graphics/decade_pySpark.png')
    plt.show()

    # Удаляем ранее созданный столбец decade перед выгрузкой датафрейма в clickhouse
    df = df.drop("decade")

    # подключаемся к clickhouse
    client = Client('clickhouse_user')

    # Преобразовываем DataFrame в список кортежей
    data_tuples = [tuple(row) for row in df.collect()]

    # Загружаем данные в ClickHouse
    client.execute('INSERT INTO houses_db.table_sp VALUES', data_tuples)

    spark.stop()


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
    drop_table_query = "DROP TABLE IF EXISTS houses_db.table_sp"
    headers = {'Content-Type': 'application/sql'}
    response = clickhouse_hook.run(endpoint='', data=drop_table_query, headers=headers)
    if response.status_code == 200:
        print("Старая таблица  успешно удалена или таблицы не существовало .")
    else:
        print("Ошибка при удалении таблицы:", response.text)

    # Создание таблицы в базе данных houses_db, если она не существует
    create_table_query = """
    CREATE TABLE IF NOT EXISTS houses_db.table_sp
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
    result = client.execute('SELECT square , full_address FROM houses_db.table_sp WHERE square > 60.0 ORDER BY square DESC LIMIT(25)')
    print(result)





create_table_task = PythonOperator(
        task_id='create_clickhouse_table',
        python_callable=create_clickhouse_table,
        dag=dag,
    )

task_query_pyspark = PythonOperator(
    task_id='query_pyspark',
    python_callable=query_pyspark,
    dag=dag,

    )

clickhouse_query_task = PythonOperator(
        task_id='sql_query_clickhouse',
        python_callable=sql_query_clickhouse,
        dag=dag,
    )



create_table_task >> task_query_pyspark >> clickhouse_query_task
