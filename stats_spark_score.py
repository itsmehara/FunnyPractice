SCORE = 0
fail = 0
passs = 0

try:
    import pyspark
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName('handson2').getOrCreate()

    df = spark.read.options(header="True", delimiter=",").csv("/projects/challenge/Result")

    df.show()

    w1 = df.filter(df["Stats"].like("%Co-variance%"))
    c1 = w1.count()

    if (c1 == 1):
        passs = passs + 50
    else:
        fail = fail + 1

    w2 = df.filter(df["Stats"].like("%Correlation%"))
    c2 = w2.count()

    if (c2 == 1):
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
