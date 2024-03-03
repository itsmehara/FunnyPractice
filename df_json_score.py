SCORE= 0
fail= 0
passs= 0

try:
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('handson2').getOrCreate()

    df=spark.read.parquet("Employees")


    w1= df.filter(df["name"].like("%Mathew%"))
    c1=w1.count()
    w2= df.filter(df["name"].like("%Rohit%"))
    c2=w2.count()
    w3= df.filter(df["name"].like("%Sumon%"))
    c3=w3.count()
    w4= df.filter(df["name"].like("%Jatin%"))
    c4=w4.count()
    w5= df.filter(df["name"].like("%Peter%"))
    c5= w5.count()


    count1= c1+c2+c3+c4+c5

    if(count1 == 5):
        passs=passs+50
    else:
        fail=fail+1

    df1= spark.read.parquet("/projects/challenge/JavaEmployees")

    w6= df1.filter(df1["name"].like("%Mathew%"))
    c6=w6.count()
    w7= df1.filter(df1["name"].like("%Peter%"))
    c7=w7.count()
    w8= df1.filter(df1["name"].like("%Sumon%"))
    c8=w8.count()


    count2= c6+c7+c8

    if(count2 == 3):
        passs=passs+50
    else:
        fail=fail+1

    SCORE= passs

except Exception as e:
    print("")
    print("FS_SCORE:" + str(SCORE) + "%")

print("")
print("FS_SCORE:" + str(SCORE) + "%")
print("")
