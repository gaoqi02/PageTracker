package mr.pc.CommonFuction;

import Bean.PathKey;
import Bean.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 我们用sessionId和pathId唯一确定了一条用户的路径。在往后的几个job中都可以用上
 * Created by gaoqida on 15-7-8.
 */
public class PageStepMapper extends Mapper<LongWritable, Text, PathKey, Vertex> {

        Vertex vertex = new Vertex();
        PathKey recordKey = new PathKey();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] cols = value.toString().split(":");

            recordKey.setPathId(cols[1]+":"+Integer.parseInt(cols[2]));
            recordKey.setTimestamp(Long.parseLong(cols[5]));

            vertex.setSessionId(cols[1]);
            vertex.setPathId(Integer.parseInt(cols[2]));
            vertex.setStepId(Integer.parseInt(cols[3]));
            vertex.setTimeStamp(Long.parseLong(cols[5]));
            vertex.setPageId(cols[4]);
            context.write(recordKey , vertex);
        }
}
