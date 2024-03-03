SCORE = 0
fail = 0
passs = 0

try:
    import pyspark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName('handson2').getOrCreate()

    df = spark.read.parquet("Age")

    df.show()

    w6 = df.filter(df["Age"].like("4"))
    c6 = w6.count()
    w7 = df.filter(df["Age"].like("%22.0%"))
    c7 = w7.count()
    w8 = df.filter(df["Age"].like("%1.414213562373095%"))
    c8 = w8.count()
    w9 = df.filter(df["Age"].like("21"))
    c9 = w9.count()
    w10 = df.filter(df["Age"].like("%24%"))
    c10 = w10.count()

    count1 = c6 + c7 + c8 + c9 + c10
    print(f'c6={c6} + c7={c7} + c8={c8} + c9={c9} + c10={c10}')
    print("1="*50 + ">", count1)

    if (count1 == 5):
        passs = passs + 50
    else:
        fail = fail + 1

    w1 = df.filter(df["summary"].like("%count%"))
    c1 = w1.count()
    w2 = df.filter(df["summary"].like("%mean%"))
    c2 = w2.count()
    w3 = df.filter(df["summary"].like("%stddev%"))
    c3 = w3.count()
    w4 = df.filter(df["summary"].like("%min%"))
    c4 = w4.count()
    w5 = df.filter(df["summary"].like("%max%"))
    c5 = w5.count()

    count2 = c1 + c2 + c3 + c4 + c5
    print(f"c1={c1} + c2={c2} + c3={c3} + c4={c4} + c5={c5}")
    print("2="*50 + ">", count2)
    if (count2 == 5):
        passs = passs + 50
    else:
        fail = fail + 1

    SCORE = passs

except Exception as e:
    print("")
    print("FS_SCORE:" + str(SCORE) + "%")

print("")
print("FS_SCORE:" + str(SCORE) + "%")
print("")
