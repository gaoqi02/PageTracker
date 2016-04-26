package mr.pc.MeanResidenceTime;

import Bean.PathKey;
import Bean.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by gaoqida on 15-7-7.
 */
public class MeanTimeReducer {

    public static class reducer
            extends Reducer<PathKey, Vertex, Text, LongWritable> {

        @Override
        protected void reduce(PathKey key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {

//            List<Vertex> list = new ArrayList<Vertex>();
//            for (Vertex vertex : values) {
//                list.add(vertex);
//            }
            long time = 0;
            //false为第一个没有前置的v所以没有钱一个浏览时间
            boolean flag = false;
            for (Vertex vertex: values){

                if (!flag) {
                    //初始化timestamp
                    time = vertex.getTimeStamp();
                    flag = true;
                } else {
                    context.write(new Text(vertex.getPageId()), new LongWritable(vertex.getTimeStamp() - time));
                    time = vertex.getTimeStamp();
                }

            }

        }
    }


    public static class reducer2
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long amount = 0;
            int count = 0;
            for (LongWritable longWritable: values) {
                amount += longWritable.get();
                count ++;
            }

            context.write(key, new LongWritable(amount/count));
        }

    }
}
