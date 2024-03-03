from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# Step 1: Import the SparkSession package

# Step 2: Create a SparkSession object
spark = SparkSession.builder \
    .appName("SaveParquet") \
    .getOrCreate()

try:
    # Step 3: Create a DataFrame with the provided data
    data = [
        (1, 'Jack', 22, 'Data Science'),
        (2, 'Luke', 21, 'Data Analytics'),
        (3, 'Leo', 24, 'Micro Services'),
        (4, 'Mark', 21, 'Data Analytics')
    ]
    columns = ["ID", "Name", "Age", "Area_of_Interest"]
    df = spark.createDataFrame(data, columns)

    # Step 4: Use describe method on the 'Age' column and observe the statistical parameters
    age_description = df.describe("Age")
    age_description.show()

    # Step 5: Save the DataFrame to a Parquet file
    output_path = "Age"
    age_description.write.mode("overwrite").parquet(output_path)

    result_df = df.select("ID", "Name", "Age").orderBy(col("Name").desc())

    result_df.write.mode("overwrite").parquet("NameSorted")
    result_df.show()

    print("DataFrame saved to Parquet file successfully.")

except Exception as e:
    print("An error occurred:", str(e))

finally:
    # Step 6: Stop the SparkSession
    spark.stop()
