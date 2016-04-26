package mr.pc.PageLineByCondition2;

import Bean.PathKey;
import Bean.Vertex;
import mr.pc.CommonFuction.GroupBySessionIdPathId;
import mr.pc.CommonFuction.PageStepMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
 *
 *
 * 第一个点前不包括路径
 * 输入的路径格式为： v1 (2,3) v2 (1,2) v3 (3,5) v4 (0,1)
 * conf中设置的两个参数 :
 * 1. v1,v2,v3,v4
 * 2.(2,3),(1,2),(3,5),(0,1)
 * * Created by gaoqida on 15-7-12.
 */
public class TaskJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //start from this page
        conf.set("vertex" , "1|7|43");
        conf.set("condition", "2,4|1,3|0,0");

        Job job = Job.getInstance(conf);
        job.setJarByClass(TaskJob.class);
        job.setJobName("TaskJob");

        job.setMapperClass(PageStepMapper.class);
        job.setMapOutputKeyClass(PathKey.class);
        job.setMapOutputValueClass(Vertex.class);

        job.setGroupingComparatorClass(GroupBySessionIdPathId.class);

        job.setReducerClass(PageLineReducer4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
//                "jdbc:mysql://localhost/weejinfu", "weejinfu", "Weejinfu123");



        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
