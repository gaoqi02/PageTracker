package mr.pc.MeanResidenceTime;

import Bean.PathKey;
import Bean.Vertex;
import mr.pc.CommonFuction.GroupBySessionIdPathId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
 * Created by gaoqida on 15-7-7.
 */
public class MeanTimeJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(MeanTimeJob.class);
        job1.setJobName("MeanTimeJob");

        job1.setMapperClass(MeanTimeMapper.mapper.class);
        job1.setMapOutputKeyClass(PathKey.class);
        job1.setMapOutputValueClass(Vertex.class);

        job1.setReducerClass(MeanTimeReducer.reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setGroupingComparatorClass(GroupBySessionIdPathId.class);

//        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
//                "jdbc:mysql://localhost/weejinfu", "weejinfu", "Weejinfu123");

        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(MeanTimeJob.class);
        job2.setJobName("MeanTimeJob");

        job2.setMapperClass(MeanTimeMapper.mapper2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setReducerClass(MeanTimeReducer.reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

//        job2.setGroupingComparatorClass(GroupBySessionIdPathId.class);

//        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
//                "jdbc:mysql://localhost/weejinfu", "weejinfu", "Weejinfu123");

        String path = args[1] + "/part-r-00000";
        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);
    }
}
