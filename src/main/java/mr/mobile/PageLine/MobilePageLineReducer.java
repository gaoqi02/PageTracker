package mr.mobile.PageLine;

import Bean.PathKey;
import Bean.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by gaoqida on 15-8-3.
 */
public class MobilePageLineReducer {

    public static class reducer
            extends Reducer<PathKey, Vertex, Text, LongWritable> {

    }
}
