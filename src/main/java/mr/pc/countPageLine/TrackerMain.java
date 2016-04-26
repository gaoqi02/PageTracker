package mr.pc.countPageLine;

import Bean.RecordKey;
import Bean.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * userid,sessionid,pathid,stepid,timestamp
 * Created by gaoqida on 15-6-29.
 */
public class TrackerMain {

    private static final Pattern TAB = Pattern.compile(",");

    public static class mapper
            extends Mapper<LongWritable, Text, RecordKey, Text> {

        private RecordKey recordKey = new RecordKey();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] cols = TAB.split(value.toString(), -1);

                recordKey.setUserid(Integer.parseInt(cols[0]));
                recordKey.setSessionId(cols[1]);
                recordKey.setTimeStamp(Long.parseLong(cols[6]));

                context.write(recordKey, new Text(value.toString()));
            } catch (Exception e){
                System.out.print(e.getMessage());
                return;
            }
        }
    }

    public static class reducer extends Reducer<RecordKey, Text, NullWritable, Text> {


        @Override
        protected void reduce(RecordKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Vertex> roots = new LinkedList<Vertex>();
//            List<String> ss = new ArrayList<String>();
//
//            for (Text vertex: values) {
//                ss.add(vertex.toString());
//            }

            //use prespm as key, Object as value
            HashMap<String, Vertex> trackerInfoHashMap = new LinkedHashMap<String, Vertex>();
            for (Text value: values){
                String[] cols = TAB.split(value.toString(), -1);
                String pageId = cols[4].split("\\.")[0];
                Vertex vertex = new Vertex(Integer.parseInt(cols[0]), cols[1], Integer.parseInt(cols[2]),
                    Double.parseDouble(cols[3]), cols[4], cols[5], pageId, Long.parseLong(cols[6]));

                //prespm--->cols[5]
                trackerInfoHashMap.put(cols[4], vertex);
                //whether pre exist
                Vertex preV = trackerInfoHashMap.get(cols[5]);
                if (preV != null){
                    preV.getVertexList().add(vertex);
                } else {
                    roots.add(vertex);
                }

            }

            List<List<Vertex>> paths = new LinkedList<List<Vertex>>();

            for (Vertex vertex: roots){
                List<Vertex> path = new LinkedList<Vertex>();
                dfs(vertex, path, paths);
            }

            for (List<Vertex> list: paths){
                for (Vertex tmp: list){
                    String str = tmp.toString();
                    context.write(null, new Text(str));

                }
            }
        }

    }

    //deep first ergodic
    public static void dfs(Vertex root, List<Vertex> path, List<List<Vertex>> paths){
        path.add(root);

        for (Vertex vertex: root.getVertexList()){
            dfs(vertex, path, paths);
        }

        if (root.getVertexList().size()==0) {
            int pathId= paths.size();
            int stepId = 0;
            List<Vertex> clonePath = new LinkedList<Vertex>();
            for (Vertex vertex: path){
                Vertex tmp = new Vertex();
                tmp.setStepId(stepId);
                tmp.setPathId(pathId);
                tmp.setSessionId(vertex.getSessionId());
                tmp.setUserid(vertex.getUserid());
                tmp.setPageId(vertex.getPageId());
                tmp.setSpm(vertex.getSpm());
                tmp.setTimeStamp(vertex.getTimeStamp());
                clonePath.add(tmp);
                stepId++;
            }

            paths.add(clonePath);
        }

        path.remove(root);

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(TrackerMain.class);
        job1.setJobName("TrackerMain");

        job1.setMapperClass(mapper.class);
        job1.setMapOutputKeyClass(RecordKey.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setReducerClass(reducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        job1.setGroupingComparatorClass(GroupBy.class);

        //arg[0]输入的文件路径  arg[1]输出的文件路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

    }

}
