import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth


def create_spark(workers: int):
    return SparkSession.builder \
        .appName("Spark Data Processor") \
        .master(f"local[{workers}]") \
        .getOrCreate()


def descriptive_stats(spark, path):
    df = spark.read.option("header", True).csv(path, inferSchema=True)

    stats = {
        "rows": df.count(),
        "columns": len(df.columns),
        "null_values": df.select([
            count(col(c)).alias(c) for c in df.columns
        ]).toPandas().to_dict(),
        "mean_values": df.select([
            mean(col(c)).alias(c) for c in df.columns if str(df.schema[c].dataType) != "StringType"
        ]).toPandas().to_dict()
    }
    return stats


def linear_regression_job(spark, path):
    df = spark.read.option("header", True).csv(path, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=df.columns[:-1],
        outputCol="features"
    )
    data = assembler.transform(df).select("features", col(df.columns[-1]).alias("label"))

    lr = LinearRegression()
    model = lr.fit(data)

    return {
        "r2": model.summary.r2,
        "rmse": model.summary.rootMeanSquaredError
    }


def logistic_regression_job(spark, path):
    df = spark.read.option("header", True).csv(path, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=df.columns[:-1],
        outputCol="features"
    )
    data = assembler.transform(df).select("features", col(df.columns[-1]).alias("label"))

    lr = LogisticRegression()
    model = lr.fit(data)

    return {
        "accuracy": model.summary.accuracy
    }


def kmeans_job(spark, path):
    df = spark.read.option("header", True).csv(path, inferSchema=True)

    assembler = VectorAssembler(
        inputCols=df.columns,
        outputCol="features"
    )
    data = assembler.transform(df)

    kmeans = KMeans(k=3)
    model = kmeans.fit(data)

    return {
        "clusters": model.summary.k
    }


def fpgrowth_job(spark, path):
    df = spark.read.text(path)
    df = df.withColumn("items", col("value").split(","))

    fp = FPGrowth(itemsCol="items", minSupport=0.2, minConfidence=0.6)
    model = fp.fit(df)

    return {
        "frequent_itemsets": model.freqItemsets.count()
    }
