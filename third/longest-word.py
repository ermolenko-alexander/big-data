from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, regexp_extract
from pyspark.sql.types import StringType

# Создание Spark сессии
spark = SparkSession.builder.appName("WikiArticles").getOrCreate()

# Загрузка файла со статьями
data = spark.read.text("wiki_articles.tsv")

# Разделение строк на столбцы
split_data = data.withColumn("url", regexp_extract("value", r"^(.*?)\t", 1)) \
    .withColumn("title", regexp_extract("value", r"\t(.*?)\t", 1)) \
    .withColumn("text", regexp_extract("value", r"\t(.*)$", 1))

# Разделение текста на отдельные слова
words = split_data.select(explode(col("text")).alias("word"))

# Находим самое длинное слово
longest_word = words.filter(col("word") != "").select(col("word"), col("word_length")).groupBy("word", "word_length") \
    .agg({"word": "count"}).withColumn("word_length", col("word_length").cast("int")).sort(col("word_length").desc()).first()

print("Самое длинное слово: {}, длина - {}".format(longest_word["word"], longest_word["word_length"]))

# Остановка Spark сессии
spark.stop()