package mr.pc.PageLineByCondition;

import Bean.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * the condition we will get is :
 * (3) 4 (2~3) 9 (2)
 * we count 3 steps before no.4 vertex,  get 2 or 3 steps between 4 and 9 vertex. count 2 steps after no.9 vertex
 * Created by gaoqida on 15-7-12.
 */
public class PageLineMapper {

    public static class mapper
            extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Vertex> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split(":");

            context.write( new Text(cols[1]+":"+cols[2]),
                    new Vertex(cols[1], Integer.parseInt(cols[2]), Integer.parseInt(cols[3]), Long.parseLong(cols[4])));
        }
    }
}
