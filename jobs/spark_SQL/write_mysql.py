#!/usr/bin/env python3
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark import SparkConf
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.config(conf=SparkConf()).appName("write-mysql-demo").getOrCreate()

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    student_rdd = spark.sparkContext.parallelize(
        [
            "3 Rongcheng M 26",
            "4 Guanhua M 27",
        ]
    ).map(lambda x: x.split(" "))

    row_rdd = student_rdd.map(
        lambda p: Row(
            int(p[0].strip()),
            p[1].strip(),
            p[2].strip(),
            int(p[3].strip()),
        )
    )

    student_df = spark.createDataFrame(row_rdd, schema)

    prop = {
        "user": "root",
        "password": "rootpassword",
        "driver": "com.mysql.cj.jdbc.Driver",
    }

    jdbc_url = "jdbc:mysql://mysql:3306/spark?useSSL=false&serverTimezone=UTC"
    student_df.write.jdbc(jdbc_url, "student", "append", prop)

    print("数据写入成功，开始读取验证...")

    jdbc_df = (
        spark.read.format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", jdbc_url)
        .option("dbtable", "student")
        .option("user", "root")
        .option("password", "rootpassword")
        .load()
    )

    jdbc_df.orderBy("id").show()
    spark.stop()


if __name__ == "__main__":
    main()
