from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, length

# Создание Spark сессии
spark = SparkSession.builder.appName("AverageWordLength").getOrCreate()

# Загрузка файла со статьями
data = spark.read.text("wiki_articles.tsv")

# Разделение текста на отдельные слова
words = data.select(explode(col("value")).alias("word"))

# Вычисление длины каждого слова
word_lengths = words.filter(col("word") != "").select(length("word").alias("word_length"))

# Находим среднюю длину слов
average_length = word_lengths.agg({"word_length": "avg"}).first()[0]

print("Средняя длина слова в статьях: {:.2f}".format(average_length))

# Остановка Spark сессии
spark.stop()