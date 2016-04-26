package mr.pc.PV;

import Bean.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
 * Created by gaoqi on 2015/7/4.
 */
public class CountPvJob {


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(CountPvJob.class);
        job1.setJobName("PvJob");

        job1.setMapperClass(CountPvMapper.mapper.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Page.class);

        job1.setReducerClass(CountPvReducer.reducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

//        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
//                "jdbc:mysql://localhost/weejinfu","weejinfu","Weejinfu123");

        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

    }
}
