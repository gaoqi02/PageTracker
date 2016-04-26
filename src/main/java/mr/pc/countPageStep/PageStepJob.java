package mr.pc.countPageStep;

import Bean.Page;
import mr.pc.CommonFuction.PageStepMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
 * 从某一个页面开发算后面几步的页面
 * Created by gaoqida on 15-7-8.
 */
public class PageStepJob {


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(PageStepJob.class);
        job1.setJobName("PageStepJob");

        job1.setMapperClass(PageStepMapper.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Page.class);

        job1.setReducerClass(PageStepReducer.reducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        //start from this page
        conf.set("pageId", args[2]);
        conf.set("start", args[3]);
        conf.set("end", args[4]);

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost/weejinfu", "weejinfu", "Weejinfu123");

        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

    }
}
