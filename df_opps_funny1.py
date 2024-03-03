
'''
In this hands on, you will start working on pyspark basics.




step1:
import sparksession package
create  a spark session object
create dataframe with the following details under the header as "ID", "Name", "Age", "Area of interest"


1, Jack, 22, 'Data Science'
2, Luke, 21, Data Analytics
3, Leo, 24, Micro Services
4, Mark, 21, Data Analytics

Step5:
Use describe method on age column and observe the stastical parameters and
save the data into a parquet file under the folder with name 'Age' inside /projects/challenge/.

Step6: select column ID, name and age, and Name should be in descending order.
save the result into a parquet file under the folder with name "NameSorted" inside /projects/challenge/.

Note:
    1. use coalesce to store the data frame as a single partition.
    2. Ensure to use the exact naming convention for the result folders

Steps to complete the handson:
1. Run your solution using the run....






'''

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Data Frame Example") \
    .getOrCreate()

Student = Row("ID", "Name", "Age", "Area of interest")
s1 = Student(1, 'Jack', 22, "Data Science")
s2 = Student(2, 'Luke', 21, "Data Analytics")
s3 = Student(3, 'Leo', 24, "Micro Services")
s4 = Student(4, 'Mark', 21, "Data Analytics")

StudentData=[s1,s2, s3, s4]
df=spark.createDataFrame(StudentData)
df.show()

df.describe(['Age']).show()
df.describe(['Age']).write.parquet("Age.parquet")

df.write.parquet("NameSorted.parquet")

