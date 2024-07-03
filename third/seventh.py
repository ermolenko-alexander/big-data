from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_extract
from pyspark.sql.types import IntegerType

# Создание Spark сессии
spark = SparkSession.builder.appName("NameAnalysis").getOrCreate()

# Загрузка данных из файла wiki_articles.tsv
data = spark.read.option("sep", "\t").csv("wiki_articles.tsv", header=False)

# Разделение текста статьи на слова
words_df = data.select(explode(split(col("_c2"), "\s+")).alias("word"))

# Фильтрация слов, начинающихся с заглавных букв (предполагаемые имена)
names_df = words_df.filter(col("word").rlike("^[А-Я][а-я]+$"))

# Вычисление частоты употребления имен
names_freq_df = names_df.groupBy("word").count().withColumnRenamed("count", "frequency")

# Сортировка по частоте употребления
sorted_names_freq_df = names_freq_df.orderBy(col("frequency").desc())

# Вывод топ-10 самых употребляемых имен
sorted_names_freq_df.show(10)

# Остановка Spark сессии
spark.stop()