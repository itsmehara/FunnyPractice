"""
import pyspark and spark session.
create spark session object
create 10 random values as a column and name the column as rand1
create another 10 random values as column and name the column as rand2
calculate the co-variance and correlation between these two columns
create new dataframe with header names as 'Stats' and 'Value'

fill the new dataframe with the obtained values as 'Co-variance' and 'Correlation'

save the resultant dataframe to a csv file with name 'Result'



"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn

spark = SparkSession.builder \
    .appName("Stats1") \
    .getOrCreate()

try:
    df = spark.range(1, 11).withColumn("rand1", randn(seed=10))
    df = df.withColumn("rand2", randn(seed=27))
    df.show()

    covariance = df.stat.cov('rand1', 'rand2')
    correlation = df.stat.corr('rand1', 'rand2')

    print(covariance, correlation, "<------------")

    summary_df = spark.createDataFrame([
        ("Co-variance", covariance),
        ("Correlation", correlation)
    ], ["Stats", "Value"])

    output_path = "Result"
    summary_df.write.mode("overwrite").csv(output_path, header=True)

except Exception as e:
    print("An error occurred:", str(e))

finally:
    spark.stop()