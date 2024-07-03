from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_extract
from pyspark.sql.types import IntegerType

# Создание Spark сессии
spark = SparkSession.builder.appName("AbbreviationAnalysis").getOrCreate()

# Загрузка данных из файла wiki_articles.tsv
data = spark.read.option("sep", "\t").csv("wiki_articles.tsv", header=False)

# Разделение текста статьи на слова
words_df = data.select(explode(split(col("_c2"), "\s+")).alias("word"))

# Фильтрация сокращений вида "пр.", "др.", и т.д.
abbreviations_df = words_df.filter(col("word").rlike("^[а-я]+[.]$"))

# Вычисление частоты сокращений
abb_freq_df = abbreviations_df.groupBy("word").count().withColumnRenamed("count", "frequency")

# Вывод результата
abb_freq_df.show()

# Остановка Spark сессии
spark.stop()