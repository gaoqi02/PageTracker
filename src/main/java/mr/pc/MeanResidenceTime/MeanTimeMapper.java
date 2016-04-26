package mr.pc.MeanResidenceTime;

import Bean.PathKey;
import Bean.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
 * use sessionId and pathId to identify a path of people
 * use ercipaixu .. to sort stepId
 * Created by gaoqida on 15-7-7.
 */
public class MeanTimeMapper {

    /**
     * recordKey 在reduce之前做group的时候按照session和path来聚合，聚合之后按照timestamp来排序。
     * 当然也可以按照第一步给的step排序，但是recordKey在第一步使用了，重写了compareTo的方法按timestamp所以没法改成按step。除非创建新的对象。
     * 这一步获得的是有序的Vertex.
     */
    public static class mapper
            extends Mapper<LongWritable, Text, PathKey, Vertex> {

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

    public static class mapper2
            extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitValue = value.toString().split("\t");

            context.write(new Text(splitValue[0]), new LongWritable(Long.parseLong(splitValue[1])));
        }
    }
}
