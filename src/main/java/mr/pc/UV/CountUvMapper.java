package mr.pc.UV;

import Bean.Page;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Created by gaoqi on 15-7-6.
 */
public class CountUvMapper {

    private static final Pattern TAB = Pattern.compile(":");

    public static class mapper
            extends Mapper<LongWritable, Text, IntWritable, Page> {

        Page page = new Page();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] cols = TAB.split(value.toString(), -1);

                page.setId(Integer.parseInt(cols[3]));
//                page.setCount(1);
                page.setSessionId(cols[1]);
                page.setUuid(cols[0]);
                page.setTimeStamp(Long.parseLong(cols[4]));

                context.write(new IntWritable(page.getId()), page);

            } catch (Exception e){
                System.out.println(e.getMessage() + "@@@@");
                return;
            }
        }
    }
}
