# Загрузка, очистка, анализ данных, построение графиков с помощью библиотек Pyspark/pandas в среде Airflow c дальнейшей выгрузкой в БД Clickhouse. С использованием контейнеризации docker-compose.

## Технологический стек:
Docker, Airflow, PySpark, Clickhouse, pandas. <br>
Использование технологии docker-compose позволяет легко и быстро развернуть всю необходимую в рамках данного проекта инфраструктуру для автоматизации процесса обработки данных.


## Исходные данные:
Имеется csv файл с данными по зданиям и сооружениям, расположенным на территории РФ и некоторых стран бывшего СССР. 
Файл доступен по ссылке: https://disk.yandex.ru/d/OdhP0RmgPWSAVw

Количество строк - 590708 строк.

## Данные в файле:
house_id (например, 196609)  ID объекта   <br>
lalitude (например, 52.358985) Координаты, широта  <br>
longitude (например, 104.218147)  Координаты, долгота <br>
maintenance_year (например, 1978)  Год постройки <br>
square (например, 2663,4)  Площадь <br>
population (например, 141)  Количество проживающих  <br>
region (например, Иркутская область) Регион  <br>
locality_name (например, Иркутск)  Населенный пункт  <br>
address (например, "ул. Кутузова, д. 17") Адрес  <br>
full_address (например, Иркутская область, г. Иркутск, ул. Кутузова, д. 17)  Полный адрес  <br>
communal_service_id (например, 11111.0 ) ID коммунальной службы  <br>
description (например, "Жилой дом в Иркутске, по адресу ул. Кутузова, д. 17, 1978 года постройки, под управлением ЖК-48.") Описание объекта  <br>



## Для обработки данных необходимо:
1. предварительно развернуть docker-compose, выложенный в данном репозитории. Все необходимые службы и библиотеки в нем прописаны.
   Будут развернуты контейнеры с airflow, clickhouse, postgres, spark, grafana, redis.
2. скопировать файлы  main_spark.py и main_pandas.py в папку /dags
3. Зайти в вэб-интерфейс airflow с учетной записью по-умолчанию, убедиться что DAG'и подгрузились в airflow.
4. Запустить любой из DAG-файлов и получить результат его отработки (аналитику в логах Python, графики, заполненную данными БД в clickhouse 
   и результат отработки SQL запроса в логах Python).
   Абсолютно все действия выполняются в среде airflow. Предварительно создавать и редактировать вручную БД с помощью какой-либо СУБД не нужно.


<br>
<br>
Выложенный в репозитории файл main_spark.py является единым DAG-файлом, выполняющим все нижеперечисленные действия:

1. Загружает файл данных (csv) в DataFrame PySpark. Выводит количество строк датафрейма.

2. Выводит часть датафрейма и его структуру на экран, для того чтобы  убедиться что данные корректно прочитаны (правильный формат, типы данных и т.п.).

3. Преобразовывает текстовые и числовые поля в соответствующие типы данных (например, дата, число).

4. Вычисляет средний и медианный год постройки зданий.

5. Определяет топ-10 областей и городов с наибольшим количеством объектов и строит соответствующие графики.

6. Находит здания с максимальной и минимальной площадью в рамках каждой области и строит соответствующие графики.

7. Определяет количество зданий по десятилетиям (например, сколько зданий построено в 1950-х, 1960-х и т.д.) и строит график.

8. Создает новую БД и таблицу в ClickHouse со схемой, которая  соответствует структуре  данных. 

9. Настраивает оединение с ClickHouse из скрипта DAG

10. Загружает обработанные данные из DataFrame в таблицу  ClickHouse.

11. Выполняет SQL скрипт в Python, который выводит топ 25 домов, у которых площадь больше 60 кв.м (airflow)


## Файлы и папки в репозитории:
### docker-compose.yml 
- docker-compose со следующими контейнерами: airflow, clickhouse, postgres, spark, grafana, redis. В нем прописана ссылка на файл Dockerfile.airflow 
Dockerfile.airflow -  файл с  кастомным образом airflow с добавлением туда openjdk и ряда библиотек airflow и python, необходимых для выполнения представленных здесь DAG'ов и работоспособности pyspark в среде airflow.

### main_spark.py 
- единый DAG - файл, в котором выполняются все вышеописанные действия, начиная с п.1. Загружаются данные из CSV файла. (Путь по-умолчанию для csv-файла, прописанный в DAG: /opt/airflow/df/). Очищаются, преобразуются, анализируются. Результаты анализа выводятся в логах Python. Строятся графики с помощью библиотек seaborn и matplotlib. Графики выгружаются в папку: /opt/airflow/df/graphics. Создается БД houses_db в Clickhouse, в ней создается таблица table_sp, в которую загружаются данные из датафрейма pyspark после их анализа. Запускается SQL скрипт который выводит топ 25 домов, у которых площадь больше 60 кв.м. Результат выполнения скрипта выводится в логах Python соответствующего задания.

### bonus/main_pandas.py 
- единый DAG - файл, в котором выполняются все вышеописанные действия, начиная с п.1, но с использованием pandas вместо pyspark. Пути для файлов те же самые. Из отличий: создается своя отдельная таблица table_pd в базе houses_db, графики seaborn/matplotlib сохраняются также в папку /opt/airflow/df/graphics, но имеют названия отличные от названий графиков, сделанных с помощью pyspark.

### graphics/
- папка с графиками, полученными в результате отработки main_spark.py и main_pandas.py.

  ### Пример аналитического графика, создаваемого скриптом:
![Пример графика](graphics/decade_pySpark.png)

