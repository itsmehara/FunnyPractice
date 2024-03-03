
from pyspark.sql.functions import rand,randn
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, min, max

spark = SparkSession.builder \
    .appName("SaveParquet2") \
    .getOrCreate()

df = spark.range(0,7)
df1 = df.select("id").orderBy(rand()).limit(4)
df1.show()

df_select = df.select("id", rand(seed=10).alias("uniform"), randn(seed=27).alias("normal"))

df_select.show()
df_select.describe('uniform', 'normal').show()

df_select.select([mean('uniform'), min('uniform'), max('uniform')]).show()

# df = sqlContext.range(0, 10).withColumn('rand1', rand(seed=10)).withColumn('rand2', rand(seed=27))
dfsql = spark.range(0, 10).withColumn('rand1', rand(seed=10)).withColumn('rand2', rand(seed=27))
dfsql.stat.cov('rand1', 'rand2')


dfsql.stat.corr('rand1', 'rand2')

# Create a DataFrame with two columns (name, item)
names = ["Alice", "Bob", "Mike"]
items = ["milk", "bread", "butter", "apples", "oranges"]
twocol_df = spark.createDataFrame([(names[i % 3], items[i % 5]) for i in range(100)], ["name", "item"])

twocol_df.stat.crosstab("name", "item").show()
print(twocol_df.count(), twocol_df.count)