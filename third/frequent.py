from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, regexp_extract

# Создание Spark сессии
spark = SparkSession.builder.appName("MostFrequentLatinWord").getOrCreate()

# Загрузка файла со статьями
data = spark.read.text("wiki_articles.tsv")

# Разделение текста на отдельные слова
words = data.select(explode(col("value")).alias("word"))

# Выбор только слов, состоящих из латинских букв
latin_words = words.filter(col("word").rlike("^[a-zA-Z]+$"))

# Находим самое частоупотребляемое слово
most_frequent_word = latin_words.filter(col("word") != "").groupBy("word").count().sort(col("count").desc()).first()

print("Самое частоупотребляемое слово из латинских букв: {}, количество упоминаний - {}".format(most_frequent_word["word"], most_frequent_word["count"]))

# Остановка Spark сессии
spark.stop()