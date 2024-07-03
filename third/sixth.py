from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, regexp_extract
from pyspark.sql.types import IntegerType

# Создание Spark сессии
spark = SparkSession.builder.appName("AbbreviationAnalysis").getOrCreate()

# Загрузка данных из файла wiki_articles.tsv
data = spark.read.option("sep", "\t").csv("wiki_articles.tsv", header=False)

# Разделение текста статьи на слова
words_df = data.select(explode(split(col("_c2"), "\s+")).alias("word"))

# Фильтрация сокращений вида "т.п.", "н.э." и подобных
abbreviations_df = words_df.filter(col("word").rlike("^[а-я]{1,2}[.]$"))

# Вычисление частоты сокращений
abb_freq_df = abbreviations_df.groupBy("word").count().withColumnRenamed("count", "frequency")

# Фильтрация сокращений по частоте
stable_abbreviations_df = abb_freq_df.filter(col("frequency") > 100)

# Вывод результата
stable_abbreviations_df.show()

# Остановка Spark сессии
spark.stop()