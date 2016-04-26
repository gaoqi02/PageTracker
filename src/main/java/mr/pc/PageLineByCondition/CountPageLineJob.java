package mr.pc.PageLineByCondition;

import Bean.Page;
import mr.pc.MeanResidenceTime.MeanTimeJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
 *
 * Created by gaoqida on 15-7-12.
 */
public class CountPageLineJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(MeanTimeJob.class);
        job1.setJobName("TaskJob");

        job1.setMapperClass(PageLineMapper.mapper.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Page.class);

        job1.setReducerClass(PageLineReducer.reducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost/weejinfu", "weejinfu", "Weejinfu123");

        //start from this page
        conf.set("start", args[2]);
        conf.set("last", args[3]);
        conf.set("first", args[4]);
        conf.set("second", args[5]);
        conf.set("third", args[6]);
        conf.set("forth", args[7]);


        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    }
}
