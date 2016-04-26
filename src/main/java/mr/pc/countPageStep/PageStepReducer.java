package mr.pc.countPageStep;

import Bean.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 从某一个页面开始后面几步的变化。
 * there we use text to stands for pagePath just like we want get page3 and step between 2,4.
 * finally we get result like : 3,4,7 \n  3,10,1 \n ...
 * this is ease to parse
 * Created by gaoqi on 15-7-8.
 */
public class PageStepReducer {

    public static class reducer extends Reducer<Text, Vertex, NullWritable, Text> {



        @Override
        protected void reduce(Text key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();

            String pageId = configuration.get("pageId");
            int start = Integer.parseInt(configuration.get("start"));
            int end = Integer.parseInt(configuration.get("end"));

            //in this path may have many page which we will solve, so this list contains this pageId's index
            List<Integer> index = new ArrayList<Integer>();
            //this list get all vertex
            List<Vertex> list = new ArrayList<Vertex>();
            int count = 1;
            for (Vertex vertex : values) {
                if (vertex.getPageId().equals(pageId)) {
                    index.add(count);
                }
                list.add(vertex);
                count++;
            }


            for (Integer i: index) {
                StringBuffer stringBuffer = new StringBuffer();
                stringBuffer.append(list.get(i).getPageId());
                int tmp = i + start; // this mean the vertex we will get
                while (tmp < i+ end) {
                    stringBuffer.append(",");
                    stringBuffer.append(list.get(tmp));
                    tmp ++;
                }

                context.write(null, new Text(stringBuffer.toString()));
            }
        }












    }
}
