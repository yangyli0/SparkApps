package li.sparkapps;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

/**
 * Created by lee on 7/3/17.
 */
public class LogAnalysisJa {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("please specify the HDFS URI:");
            System.exit(1);
        }

        logAnalysis(args[0]);
    }

    public static void logAnalysis(String path) {
        SparkConf conf = new SparkConf().setAppName("WebLogAnalysis");
        // 设置时间间隔为1秒
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Duration.apply(1000));
        JavaDStream lines = jssc.textFileStream(path);

        // 1.统计总PV
        lines.count().print();

        // 2.各IP PV
        lines.map(new Function() {
            @Override
            public Object call(Object v1) throws Exception {
                return null;
            }
        })



    }
}
