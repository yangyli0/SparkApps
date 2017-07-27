package li.sparkapps.codec;

import scala.Tuple2;

import java.util.List;

/**
 * Created by lee on 7/27/17.
 */
public class JavaCodec {
    public static String listToJson(List<Tuple2<String, Integer>> tupleList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Tuple2<String, Integer> tuple: tupleList) {
            sb.append("{\"");
            sb.append(tuple._1());
            sb.append("\":");
            sb.append(tuple._2());
            sb.append("},");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append("]");

        return sb.toString();

    }
}
