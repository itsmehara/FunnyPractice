"""
import the spark session package
create spark session object
read the json file, and create dataframe with the json data.
display the dataframe.
save dataframe to a parquet file with name "Employees"
From the dataframe display the associates who are mapped to "JAVA" stream.
Save the resultant data frame to a parquet file with name "JavaEmployees"

read the json file, and create dataframe with the json data.
From the dataframe display the associates who are mapped to "JAVA" stream.

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SaveParquet2") \
    .getOrCreate()

try:

    # Step 2: Read the JSON file into a DataFrame
    json_file_path = "emp.json"
    df = spark.read.json(json_file_path)
    df.write.mode("overwrite").parquet("Employees")
    df.show()
    # Step 3: Filter the associates who are mapped to the "JAVA" stream
    java_associates = df.filter(df.stream == "JAVA")

    # Step 4: Display the filtered DataFrame
    java_associates.show()
    java_associates.write.mode("overwrite").parquet("JavaEmployees")

except Exception as e:
    print("An error occurred:", str(e))

finally:
    # Step 6: Stop the SparkSession
    spark.stop()
