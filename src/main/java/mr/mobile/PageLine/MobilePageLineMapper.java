package mr.mobile.PageLine;

import MobileBean.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * 移动端的对象
 * Page的名称,Page表明的key,Page的详细信息，时间戳，SessionId,经度,纬度,设备Id
 * 其中设备id和sessionId可以单独唯一确定一个当前用户，不会用重复的。
 * mapper阶段将sessionId和deviceId
 * Created by gaoqida on 15-8-3.
 */
public class MobilePageLineMapper {

    private static final Pattern TAB = Pattern.compile("!@#");

    public static class mapper
            extends Mapper<LongWritable, Text, IntWritable, Vertex> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] cols = TAB.split(value.toString(), -1);

//                recordKey.setUserid(Integer.parseInt(cols[0]));
//                recordKey.setSessionId(cols[1]);
//                recordKey.setTimeStamp(Long.parseLong(cols[6]));
//
//                context.write(recordKey, new Text(value.toString()));
            } catch (Exception e){
                System.out.print(e.getMessage());
                return;
            }
        }
    }
}
