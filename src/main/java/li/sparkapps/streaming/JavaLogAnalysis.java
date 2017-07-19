package li.sparkapps.streaming;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lee on 7/3/17.
 */
public class JavaLogAnalysis {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("please specify the HDFS URI:");
            System.exit(1);
        }

        logAnalysis(args[0]);
    }



    public static void logAnalysis(String path) {
        SparkConf conf = new SparkConf().setAppName("WebLogAnalysis").setMaster("local[*]");
        // 设置时间间隔为1秒
        //JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
        JavaDStream<String> lines = jssc.textFileStream(path);

        // 1.统计总PV
        lines.count().print();

        // 2.各IP PV TODO: 用lambda代替
        JavaPairDStream<String, Integer> pairs = lines.mapToPair(
                str -> new Tuple2<>(str.split(" ")[0], 1)
        );

        JavaPairDStream<String, Integer> reducePairs = pairs.reduceByKey(
                (v1, v2) -> v1 + v2
        );

        PairFunction<Tuple2<String, Integer>, Integer, String> reverse = item -> item.swap();
        PairFunction<Tuple2<Integer, String>, String, Integer> reverseBack = item -> item.swap();
        JavaPairDStream<String, Integer> transformPairs = reducePairs.transformToPair(
                pairRDD -> pairRDD.mapToPair(reverse)
                        .sortByKey()
                        .mapToPair(reverseBack)
        );
        transformPairs.print();

        // 3.搜索引擎pv
        JavaDStream<String> refer = lines.map(str -> str.split("\"")[3]);

        JavaPairDStream<String,String> searchEngineInfo = refer.mapToPair( r -> {
            String[] f = r.split("/");
            Map<String, String>  searchEngines = new HashMap<>();
            searchEngines.put("www.google.cn", "q");
            searchEngines.put("www.yahoo.com", "p");
            searchEngines.put("cn.bing.com", "q");
            searchEngines.put("www.baidu.com", "wd");
            searchEngines.put("www.sougou.com", "query");

            if (f.length > 2) {
                String host =  f[2];
                if (searchEngines.containsKey(host)) {
                    String query = r.split("\\?")[1];
                    int cnt = 0;
                    if (query.length() > 0) {
                        List<String> temp = new ArrayList<>();
                        for (String val : query.split("&")) {
                            if (val.indexOf(searchEngines.get(host)+"=") == 0) {
                                temp.add(val);
                                cnt++;
                            }
                        }
                        String[] arr_search_q = new String[cnt];
                        arr_search_q = temp.toArray(arr_search_q);
                        if (arr_search_q.length > 0)
                            return new Tuple2<>(host, arr_search_q[0].split("=")[1]);
                        return new Tuple2<>(host, "");
                    } return new Tuple2<>(host, "");
                } return new Tuple2<>("", "");
            } return new Tuple2<>("", "");}
        );

        searchEngineInfo.filter(pair -> pair._1.length() > 0)
                .mapToPair(p -> new Tuple2<>(p._1, 1))
                .reduceByKey(((v1, v2) -> v1+v2)).print();

        // 终端类型PV
        JavaDStream<String> devices =  lines.map(str -> str.split("\"")[5]);
        JavaPairDStream<String, Integer> devicesPair = devices.mapToPair(agent -> {
            String[] facilities = {"iPhone", "Android"};
            String de = "Default";
            for (String str : facilities) {
                if (agent.indexOf(str) != -1)
                    return new Tuple2<>(str, 1);
            }
            return new Tuple2<>(de, 1);
        });

        devicesPair.reduceByKey(((v1, v2) -> v1+v2)).print();

        // 访问页面PV
        JavaDStream<String> pages = lines.map(str -> str.split("\"")[1].split(" ")[1]);
        JavaPairDStream<String, Integer> pagePairs = pages.mapToPair(str -> new Tuple2<>(str, 1));

        pagePairs.reduceByKey(((v1, v2) -> v1+v2)).print();
        jssc.start();
        jssc.awaitTermination();


    }


}

