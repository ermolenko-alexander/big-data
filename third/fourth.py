from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, when, regexp_extract

# Создание Spark сессии
spark = SparkSession.builder.appName("FilteredWords").getOrCreate()

# Загрузка файла со статьями
data = spark.read.text("wiki_articles.tsv")

# Разделение текста на отдельные слова
words = data.select(explode(col("value")).alias("word"))

# Фильтрация слов, которые начинаются с большой буквы
capitalized_words = words.filter(col("word") != "").withColumn("is_capitalized", when(col("word").rlike("^[A-Z].*"), 1).otherwise(0))

# Группировка слов по количеству вхождений
word_counts = capitalized_words.groupby("word").agg({"is_capitalized": "sum"}).withColumnRenamed("sum(is_capitalized)", "capitalized_count")

# Фильтрация слов, которые начинаются с большой буквы более чем в половине случаев и встречаются более 10 раз
filtered_words = word_counts.filter((col("capitalized_count") > 10) & (col("capitalized_count") > col("capitalized_count") / 2))

filtered_words.show()

# Остановка Spark сессии
spark.stop()