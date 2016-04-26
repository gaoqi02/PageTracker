package mr.pc.UV;

import Bean.Page;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


/**
 * pageId as key , we use page.uuid as a key in hash,
 * so we can get the unique visitor in this day by someone...
 * pv : how many times people visit this page.
 * uv : how many people (uuid or userid stands for a person and unique ) visit this page.
 * Created by gaoqi on 2015/7/4.
 */
public class CountUvReducer {

    /**
     * 将session和timestamp 做为key
     */
    public static class reducer extends Reducer<IntWritable, Page, IntWritable, IntWritable> {


        @Override
        protected void reduce(IntWritable key, Iterable<Page> values, Context context) throws IOException, InterruptedException {

            HashSet<String> pageMap = new HashSet<String>();

            for (Page page : values) {
                String value = page.getUuid();

                if (!pageMap.contains(value)) {
                    pageMap.add(value);

                }
            }

            context.write(key, new IntWritable(pageMap.size()));
        }
    }
}
