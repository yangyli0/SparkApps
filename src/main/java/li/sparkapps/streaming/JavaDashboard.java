package li.sparkapps.streaming;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import li.sparkapps.codec.JavaCodec;


/**
 * Created by lee on 7/27/17.
 */
public class JavaDashboard {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: Dashboard <zkQuorum> <group> <topics ><numThreads>");
            System.exit(1);
        }
        String zkQuorum = args[0];
        String groupId = args[1];
        String topics = args[2];
        int numThreads = Integer.parseInt(args[3]);
        dashboard(zkQuorum, groupId, topics, numThreads);
    }

    public static void dashboard(String zkQuorum, String groupId, String topics, int numThreads) {
        final Pattern SPACE = Pattern.compile(" ");
        SparkConf conf = new SparkConf().setAppName("Dashboard").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.checkpoint("./checkpoint");
        Map<String, Integer> topicMap = new HashMap<>();
        for (String topic: topics.split(",")) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, groupId, topicMap);

       //messages.print();  调试用


        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> pair) {
                return pair._2();
            }
        });

        //lines.print();


        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s));
            }
        });

        //words.print();


        JavaPairDStream<String, Integer> pairs = words.mapToPair(str -> new Tuple2<>(str, 1));

        //pairs.print();  // 调试用


        JavaPairDStream<String, Integer> reducedPairs = pairs.reduceByKeyAndWindow(((v1, v2) -> v1 + v2), ((v1, v2) -> v1 - v2),
                new Duration(1000), new Duration(1000), 1, new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> v1) throws Exception {
                       if (v1._1().equals("1") || v1._1().equals("0"))
                            return true;
                        return false;
                    }
                });

       // reducedPairs.print();   // debug

        reducedPairs.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> v1) throws Exception {
                if (v1.count() != 0) {
                    Map<String, Object> props = new HashMap<>();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            "org.apache.kafka.common.serialization.StringSerializer");
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                            "org.apache.kafka.common.serialization.StringSerializer");
                    KafkaProducer<String, String> producer = new KafkaProducer<>(props);


                    List<Tuple2<String, Integer>> rddList = v1.collect();
                    String str = JavaCodec.listToJson(rddList);



                    ProducerRecord<String, String> message = new ProducerRecord<>("result", null, str);
                    producer.send(message);

            }
            return null;
        }
    });

        jssc.start();
        jssc.awaitTermination();
    }
}
