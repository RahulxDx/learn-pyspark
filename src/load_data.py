from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("load-csv").master("local[*]") \
    .getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\Rahul\Desktop\Centillion Training\learn-pyspark\data\household_power_consumption_household_power_consumption.csv")

print(df.show())
df.printSchema()

column1 = df.Date
column2 = df['Date']
column3 = col('Date')

df.select(column1, column2, column3).show()
df.select("Date", "Time", "Voltage").show()

date_as_string = df["Date"].cast(StringType()).alias("date")

df.select(df["Date"], date_as_string).show()

