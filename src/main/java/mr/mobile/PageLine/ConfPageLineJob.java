package mr.mobile.PageLine;

import Bean.RecordKey;
import mr.pc.countPageLine.GroupBy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by gaoqida on 15-8-3.
 */
public class ConfPageLineJob {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(ConfPageLineJob.class);
        job1.setJobName("TrackerMain");

        job1.setMapperClass(MobilePageLineMapper.mapper.class);
        job1.setMapOutputKeyClass(RecordKey.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(MobilePageLineReducer.reducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setGroupingComparatorClass(GroupBy.class);

        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

    }
}
