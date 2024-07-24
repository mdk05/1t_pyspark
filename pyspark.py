from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, IntegerType, DoubleType
import random
import datetime
import pandas as pd
import glob

spark = SparkSession.builder.appName("SyntheticDataGenerator").getOrCreate()

# Функции для генерации данных
def generate_product():
    products = ["Товар1", "Товар2", "Товар3", "Товар4", "Товар5"]
    return random.choice(products)

def generate_quantity():
    return random.randint(1, 10)

def generate_price():
    return round(random.uniform(10.0, 100.0), 2)

def generate_date():
    start_date = datetime.date.today() - datetime.timedelta(days=365)
    end_date = datetime.date.today()
    return start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))

# Создание UDF (User Defined Functions)
generate_product_udf = udf(generate_product, StringType())
generate_quantity_udf = udf(generate_quantity, IntegerType())
generate_price_udf = udf(generate_price, DoubleType())
generate_date_udf = udf(generate_date, StringType())

# Параметры генерации
num_rows = 1000  # Минимальное количество строк

# Генерация данных
data = [(generate_date(), random.randint(1000, 9999), generate_product(), generate_quantity(), generate_price()) for _ in range(num_rows)]

# Создание DataFrame
schema = ["Дата", "UserID", "Продукт", "Количество", "Цена"]
df = spark.createDataFrame(data, schema=schema)

# Сохранение данных в CSV файл
df.write.csv("synthetic_data2.csv", header=True)
df.show(5)


# Найти все части CSV файлов
csv_files = glob.glob("synthetic_data.csv/part-*.csv")

# Объединить все части в один DataFrame
df = pd.concat([pd.read_csv(f) for f in csv_files])

# Сохранить объединенный DataFrame в один CSV файл
df.to_csv("synthetic_data_combined.csv", index=False)


# Завершение работы SparkSession
spark.stop()
