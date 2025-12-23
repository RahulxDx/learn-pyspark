from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("load-csv").master("local[*]") \
    .getOrCreate()

PATH = r"C:\Users\Rahul\Desktop\Centillion Training\learn-pyspark\data"

def load_power_data(file_name: str):
    df = spark.read.option("header", True).csv(f"{PATH}\\{file_name}")

    return df.select(
        to_date(col("Date"), "M/d/yy").alias("date"),
        col("Voltage").cast(DoubleType()).alias("voltage")
    )

df = load_power_data(
    "household_power_consumption_household_power_consumption.csv"
)

date_plus_2 = date_add(col("date"), 2)
date_as_string = date_plus_2.cast(StringType())
concat_column = concat(lit("Date is "), date_as_string)

# df.select(col("date"), concat_column.alias("date_plus_2")).show(10)

df.createOrReplaceTempView("df")
# spark.sql("select * from df").show()

# df.sort(df["date"].desc, df["voltage"]).show()

df.groupby("date").max("voltage").show()
df.groupby(year(df["date"])).agg(max("voltage"), avg("voltage")).show()

w = Window.orderBy("date")

df.withColumn(
    "prev_voltage",
    lag("voltage", 1).over(w)
).show(5)

w = Window.partitionBy("date").orderBy(col("voltage").desc())

df.withColumn(
    "rn",
    row_number().over(w)
).filter(col("rn") == 1).show()
