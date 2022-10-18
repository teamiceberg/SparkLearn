from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
spark = (SparkSession
        .builder
        .appName("word count")
        .getOrCreate())
lines  = (spark
            .readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        )

words =  lines.select(explode(split(lines.value, " ")).alias("word"))

word_counts = words.groupBy("word").count()

query = (word_counts
.writeStream
.format("kafka")
.option("topic", "output"))