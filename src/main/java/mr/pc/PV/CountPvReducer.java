package mr.pc.PV;

import Bean.Page;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


/**
 * pageId as key , we use page.Session and page.timestamp as a key in hash,
 * so we can get the page view in this time by someone...
 * pv : how many times people visit this page.
 * uv : how many people (uuid or userid stands for a person ) visit this page.
 * Created by gaoqi on 2015/7/4.
 */
public class CountPvReducer {

    /**
     * 将session和timestamp 做为key
     */
    public static class reducer extends Reducer<IntWritable, Page, IntWritable, IntWritable> {


        @Override
        protected void reduce(IntWritable key, Iterable<Page> values, Context context) throws IOException, InterruptedException {

            HashSet<String> pageMap = new HashSet<String>();

            for (Page page : values) {
                //sessionid和时间戳能代表一次访问
                String value = page.getSessionId()+":"+page.getTimeStamp();

                if (!pageMap.contains(value)) {
                    pageMap.add(value);

                }
            }

            context.write(key, new IntWritable(pageMap.size()));
        }
    }
}
