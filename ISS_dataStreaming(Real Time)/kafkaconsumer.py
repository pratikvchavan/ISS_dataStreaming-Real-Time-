import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import time

if __name__ == "__main__":

    spark = SparkSession.builder.master("local").appname("Kafka spark demo").gerOrCreate()

    sc = spark.sparkContext()

    ssc = StreamingContext(sc, 20)

    message = KafkaUtils.createDirectStream(ssc, topics=["testtopic"],
                                            kafkaparams={"metadata.broker.list": "localhost:9092"})

    data = message.map(lambda x: x[1])


    def functordd(rdd):
        try:
            rdd1 = rdd.map(lambda x: json.loads(x))
            df = spark.read.json(rdd1)
            df.show()
            df.createOrReplaceTempview("Test")
            df1 = spark.sql("Select iss_position.latitude,iss_position.longitude,message,timestamp from Test")
            //df1.write.format('csv').mode('append').save("Testing") //If you want save data in csv format.

//Mysql connectivity

            df1.write.format("jdbc")
                .options(Map(
                    "url" -> "jdbc:mysql://localhost:3306/dbname",
                    "driver" -> "com.mysql.jdbc.Driver",
                    "dbtable" -> "table_name",
                    "user" -> "root",
                    "password" -> "root"
                 ))
                .mode("append")
                .save()

        except:
            pass


    data.froeachRDD(functordd)

    ssc.start()
    ssc.awaitTermination()