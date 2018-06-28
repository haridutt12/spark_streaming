//package org.apache.pulsar.spark.example;

import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.spark.SparkStreamingPulsarReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Consumer {

    public static void main(String[] args)  throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pulsar-spark");

     JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        ClientConfiguration clientConf = new ClientConfiguration();
        ConsumerConfiguration consConf = new ConsumerConfiguration();
        String url = "pulsar://localhost:6650/";
        String topic = "persistent://sample/standalone/ns1/my-topic";
        String subs = "sub1";

        JavaReceiverInputDStream<byte[]> msgs = jssc.receiverStream(new SparkStreamingPulsarReceiver(clientConf, consConf, url, topic, subs));

        JavaDStream<String> message = msgs.flatMap((FlatMapFunction<byte[], String>) (byte[] msg) -> {
            return Arrays.asList(new String(msg)).iterator();
            //return 10;
        });

//          JavaDStream<String> message = msgs.flatMap((FlatMapFunction<byte[], String>));
//        JavaDStream<Integer> numOfPulsar = isContainingPulsar.reduce(
//                (Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);
//
//        numOfPulsar.print();


            message.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
            JavaRDD<Row> rowRDD = rdd.map((Function<String, Row>) msg -> {
                Row row =  RowFactory.create(msg);
                return row;
            });

            //Create Schema
            StructType schema = DataTypes.createStructType(new StructField[] {DataTypes.createStructField("Message", DataTypes.StringType
                    , true)});

            //Get Spark 2.0 session
            SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());

            Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
            msgDataFrame.printSchema();
            msgDataFrame.show();
        });


        jssc.start();
        jssc.awaitTermination();
    }
}

class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}