from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window 

raw_data_path = "/Volumes/lakehouse/01_raw/raw"

source_schema = StructType([
    StructField('bbox', ArrayType(DoubleType(), True), True),
    StructField('features', ArrayType(
        StructType([
            StructField('geometry', StructType([
                StructField('coordinates', ArrayType(DoubleType(), True), True),
                StructField('type', StringType(), True)
            ]), True),
            StructField('id', StringType(), True),
            StructField('properties', StructType([
                StructField('alert', StringType(), True),
                StructField('cdi', DoubleType(), True),
                StructField('code', StringType(), True),
                StructField('detail', StringType(), True),
                StructField('dmin', DoubleType(), True),
                StructField('felt', LongType(), True),
                StructField('gap', LongType(), True),
                StructField('ids', StringType(), True),
                StructField('mag', DoubleType(), True),
                StructField('magType', StringType(), True),
                StructField('mmi', DoubleType(), True),
                StructField('net', StringType(), True),
                StructField('nst', LongType(), True),
                StructField('place', StringType(), True),
                StructField('rms', DoubleType(), True),
                StructField('sig', LongType(), True),
                StructField('sources', StringType(), True),
                StructField('status', StringType(), True),
                StructField('time', LongType(), True),
                StructField('title', StringType(), True),
                StructField('tsunami', LongType(), True),
                StructField('type', StringType(), True),
                StructField('types', StringType(), True),
                StructField('tz', StringType(), True),
                StructField('updated', LongType(), True),
                StructField('url', StringType(), True)
            ]), True),
            StructField('type', StringType(), True)
        ]), True
    ), True),
    StructField('metadata', StructType([
        StructField('api', StringType(), True),
        StructField('count', LongType(), True),
        StructField('generated', LongType(), True),
        StructField('status', LongType(), True),
        StructField('title', StringType(), True),
        StructField('url', StringType(), True)
    ]), True),
    StructField('type', StringType(), True)
])

@dp.table(
    name = "lakehouse.`02_bronze`.sdp_earthquake_source_data",
    comment= """
    "Earthquake data from USGS
    - raw data is streamed via autoloader in frm the raw volume
    """

)
def sdp_earthquake_source_data():
    bronze_source = (
        spark.readStream.format("cloudfiles")
        .option("cloudFiles.format", "json")
        .schema(source_schema)
        .load(raw_data_path)
    )
    # return df
    bronze_source_exploded = bronze_source.select(F.explode('features').alias('features'))

    bronze_source_exploded_1  = bronze_source_exploded.select(
        "features.properties.*",
        "features.id",
        F.col("features.geometry.coordinates")[0].alias("longitude"),
        F.col("features.geometry.coordinates")[1].alias("latitude"),
        F.col("features.geometry.coordinates")[2].alias("depth")
    )

    source_transform = (
        bronze_source_exploded_1.withColumn("time", F.from_unixtime(F.col("time") / 1000).cast("timestamp"))
        .withColumn("updated", F.from_unixtime(F.col("updated") / 1000).cast("timestamp"))
        .withColumn("nst", F.col("nst").cast("double"))
        .withColumn("sig", F.col("sig").cast("double"))
        .withColumn("tsunami", F.col("tsunami").cast("double"))
        .withColumn("felt", F.col("felt").cast("double"))
    )
    return source_transform
