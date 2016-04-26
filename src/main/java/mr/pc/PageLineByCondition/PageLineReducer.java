package mr.pc.PageLineByCondition;

import Bean.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by gaoqida on 15-7-12.
 */
public class PageLineReducer {

    public static class reducer extends Reducer<Text, Vertex, NullWritable, Text> {

        //the first point
        List<Integer> firstVs = new ArrayList<Integer>();
        //the second point
        List<Integer> secondVs = new ArrayList<Integer>();
        //all points list
        List<Vertex> allV = new ArrayList<Vertex>();

        String start = "";
        int last = 0;
        int first = 0;
        int second = 0;
        int third = 0;
        int forth = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();

            start = configuration.get("start");
            last = Integer.parseInt(configuration.get("last"));
            first = Integer.parseInt(configuration.get("first"));
            second = Integer.parseInt(configuration.get("second"));
            third = Integer.parseInt(configuration.get("third"));
            forth = Integer.parseInt(configuration.get("forth"));
        }

        @Override
        protected void reduce(Text key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (Vertex vertex : values) {
                vertex.setStepId(count);
                allV.add(vertex);
                if (vertex.getPageId() == start) {
                    firstVs.add(count);
                } else if (vertex.getPageId().equals(last)) {
                    secondVs.add(count);
                }
            }

            IdentityHashMap<Integer, Integer> hashMap = getCouple(firstVs,secondVs);

            Iterator iterator = hashMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                int firstVertex = (Integer)entry.getKey();
                int secondVertex = (Integer)entry.getValue();

                List<Vertex> list = new ArrayList<Vertex>();
                //we add before startV points in list
                for (int i = first; i >= 0 ; i++) {
                    if (firstVertex - i < 0) {
                        break;
                    }
                    list.add(firstVertex - i >= 0 ? allV.get(firstVertex-i) : null);
                }
                //middle Vertex
                for (int i = firstVertex; i <= secondVertex; i++) {
                    list.add(allV.get(i));
                }
                //add after lastV points in list
                for (int i =0; i <= first ; i++) {
                    if (secondVertex + i > allV.size()) {
                        break;
                    }
                    list.add( allV.get(firstVertex-i));
                }

            }

        }

        //IdentityHashMap contains chongfu de key
        public IdentityHashMap getCouple(List<Integer> firstVs, List<Integer> secondVs) {

            IdentityHashMap<Integer, Integer> hashMap = new IdentityHashMap<Integer, Integer>();

            for (Integer i: firstVs) {

                for (Integer j : secondVs) {
                    if (j - i > second && j - i < third) {
                        hashMap.put(i, j);
                    }
                    if (j - i > third) {
                        break;
                    }
                }
            }

            return hashMap;
        }
    }































}
