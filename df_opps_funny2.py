# Put your code here
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Data Frame Example") \
    .getOrCreate()

data = [
    (1, 'Jack', 22, 'Data Science'),
    (2, 'Luke', 21, 'Data Analytics'),
    (3, 'Leo', 24, 'Micro Services'),
    (4, 'Mark', 21, 'Data Analytics')
]
columns = ["ID", "Name", "Age", "Area of interest"]
df = spark.createDataFrame(data, columns)

age_clr_df = df.na.drop(subset=["Age"])
age_description = age_clr_df.describe("Age")
df.show()
age_clr_df.show()
age_description.describe(['Age']).show()
age_description.describe(['Age']).write.parquet("Age")

result_df = df.select("ID", "Name", "Age").orderBy(col("Name").desc())

result_df.write.parquet("NameSorted")
