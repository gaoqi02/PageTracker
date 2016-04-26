package mr.pc.PageLineByCondition2;

import Bean.PathKey;
import Bean.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by gaoqida on 15-7-12.
 */
public class PageLineReducer extends Reducer<PathKey, Vertex, NullWritable, Text> {


    private static final Pattern SPLIT = Pattern.compile("\\|");

    /**
     * 用递归的方法去判断切片是否合法，在生成切片的时候去做递归处理，
     * 每加入一个条件就做一次处理，例如： 1 （2,4） 3 （3,5） 8.... 在V1  和  V3 中的范围在2，4之间
     * 如果2，4之间有多个3的话就将 就将这两个3放入list 3作为root去处理后面的8,遍历这个list看看是否合法。。。
     */
    //要处理的点的list--->pageId
    String[] vertexArray;
    //要处理的点与点之间的间隔
    String[] conditionArray;
    //两个页面之间的间距  0是小  1是大的
    List<String[]> copeList;

    //这里用一个hashSet来过滤切片中的是否包含这些点。可以用重复，这只是看这些点是否符合需求，如果匹配的size<这个hashSet的话就证不符合
    HashSet<Integer> vertexSet;

    int maxLength = 0;
    int shorLength = 0;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        try {
            String s = configuration.get("condition");
            conditionArray = SPLIT.split(configuration.get("condition").trim(), 0);
            vertexArray = SPLIT.split(configuration.get("vertex"), 0);
        } catch (Exception e) {
            System.out.print(e.getMessage()+"@@@@@@@@@");
        }


        copeList = new ArrayList<String[]>();

        vertexSet = new HashSet<Integer>();

        for (String s: vertexArray) {
            int pageId = Integer.parseInt(s);

            vertexSet.add(pageId);
            String[] tmp = conditionArray[copeList.size()].split(",");
            //1下表表示范围内最大的一个元素
            maxLength += Integer.parseInt(tmp[1]);
            //0表示最小的一个元素
            shorLength += Integer.parseInt(tmp[0]);
            //这个map就如矩阵一样  v ---> 到下一个v的范围
            copeList.add(tmp);
        }
    }

    @Override
    protected void reduce(PathKey key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {

        //记录了起点的step_id
        HashSet<Integer> hashSet = new HashSet<Integer>();
        List<Vertex> allVs = new ArrayList<Vertex>();
//
//        for (Vertex vertex: values) {
//            allVs.add(vertex);
//        }


        //计算当前点的位置
        int index = 0;

        //这个reduce是从开始固定位置.所以identityHashMap的第一个点就是起始位置.若不是则代表path中无此点。
        for (Vertex vertex : values) {

            Vertex tmp = new Vertex();
            tmp.setPageId(vertex.getPageId());
            tmp.setSessionId(vertex.getSessionId());
            tmp.setStepId(vertex.getStepId());
            tmp.setTimeStamp(vertex.getTimeStamp());
            tmp.setPageId(vertex.getPageId());
            allVs.add(tmp);
            if (!vertex.getPageId().equals(vertexArray[0])){
                index ++;
                continue;
            }
            hashSet.add(index);
            index ++;

        }

        List<List<Vertex>> paths = new ArrayList<List<Vertex>>();

        //起始点的位置
        for (Integer integer: hashSet) {
            List<Vertex> path = new ArrayList<Vertex>();

            if (!buildPaths(allVs.get(integer), path, paths, allVs, 0)){
                continue;
            }
        }

        for (List<Vertex> list: paths) {
            for (Vertex vertex: list) {

                context.write(null, new Text(vertex.toString()));
            }
        }
    }


    /**
     * 因为在范围内可能会有相同的所需点，所以不能直接取。用递归。
     * 这个需要看看 相同的点仍然是一个切片所以是OK的。
     * @param root 开始计算的顶点，这个定点可能有很多合理的点list
     * @param path 当前去valid的路径，看看这条路径是否OK
     * @param paths 装所有路径的pathList
     * @param allVs 所有的点集
     */
    public boolean buildPaths(Vertex root, List<Vertex> path, List<List<Vertex>> paths,
                           List<Vertex> allVs, int index){
        path.add(root);
        try {
            //将小于最小范围内的点加入list
            for (int i = root.getStepId() + 1; i<= root.getStepId() + Integer.parseInt(copeList.get(index)[0]); i++) {
                path.add(allVs.get(i));
            }
            int start = root.getStepId() + Integer.parseInt(copeList.get(index)[0]);
            int end = root.getStepId() + Integer.parseInt(copeList.get(index)[1]);
            List<Vertex> childList = new ArrayList<Vertex>();

            boolean flag = false;
            for (int i = start; i <= end; start++) {
                if (vertexArray[index].equals(allVs.get(i).getPageId())) {
                    flag = true;
                }
                childList.add(allVs.get(i));
            }

            if (!flag) {
                return false;
            }

            index ++;
            for (Vertex vertex : childList) {
                if (!buildPaths(vertex, path, paths, allVs, index)) {
                    return false;
                }
            }

            if (childList.size() == 0) {
                List<Vertex> clonePath = new ArrayList<Vertex>();

                for (Vertex vertex : path) {
                    Vertex tmp = new Vertex();
                    tmp.setPageId(vertex.getPageId());
                    tmp.setPathId(vertex.getPathId());

                    clonePath.add(tmp);
                }
                paths.add(clonePath);
            }

            path.remove(root);

            return true;
        } catch (Exception e) {
            return false;
        }


    }


    /**
     * 首先生成一个切片然后再拿这个切片去对比内容
     * 非递归的方法去做处理
     */
    public static class reducer2 extends Reducer<Text, Vertex, NullWritable, Text> {

        //要处理的点的list--->pageId
        String[] vertexArray;
        //要处理的点与点之间的间隔
        String[] conditionArray;
        //两个页面之间的间距  []0是小  1是大的
        IdentityHashMap<Integer,String[]> identityHashMap;

        //这里用一个hashSet来过滤切片中的是否包含这些点。可以用重复，这只是看这些点是否符合需求，如果匹配的size<这个hashSet的话就证不符合
        HashSet<Integer> vertexSet;

        int maxLength = 0;
        int shorLength = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            vertexArray = SPLIT.split(configuration.get("vertex"), -1);

            conditionArray = SPLIT.split(configuration.get("condition"), -1);

            identityHashMap = new IdentityHashMap<Integer, String[]>();

            vertexSet = new HashSet<Integer>();

            for (String s: vertexArray) {
                int pageId = Integer.parseInt(s);

                vertexSet.add(pageId);
                String[] tmp = conditionArray[identityHashMap.size()].split(",");
                //1下表表示范围内最大的一个元素
                maxLength += Integer.parseInt(tmp[1]);
                //0表示最小的一个元素
                shorLength += Integer.parseInt(tmp[0]);
                //这个map就如矩阵一样  v ---> 到下一个v的范围
                identityHashMap.put(pageId, tmp);
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {

            //记录了起点的step_id
            List<Integer> startVList = new ArrayList<Integer>();
            List<Vertex> allVs = new ArrayList<Vertex>();

            //计算当前点的位置
            int index = 0;

            //这个reduce是从开始固定位置.所以identityHashMap的第一个点就是起始位置.若不是则代表path中无此点。
            for (Vertex vertex : values) {

                allVs.add(vertex);
                if (vertex.getPageId().equals(vertexArray[0])){
                    index ++;
                    continue;
                }
                startVList.add(index);
                index ++;

            }

            //承载着所有合理的路径
            List<List<Vertex>> lists = new ArrayList<List<Vertex>>();

            for (Integer i: startVList) {

                for (int end = shorLength; index <= maxLength; index ++) {
                    //这个是对应的切片
                    List<Vertex> list = new ArrayList<Vertex>();

                    //去看这些点是否在这个map中出现，如果flag小于等于这个map的size就表明指定的点并没有完全出现
                    int flag = 0;
                    for (int count = i; count<= end; count++) {
                        list.add(allVs.get(count));

                        if (identityHashMap.containsKey(allVs.get(count).getPageId())){
                            flag++;
                        }

                    }

                    //判断所有的点是否全部包含指定的点。
                    if (flag < identityHashMap.size()) {continue;}

                    //循环所有的条件看是否OK。。
                    Iterator iterator = identityHashMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry map = (Map.Entry)iterator.next();
                        int pageId = (Integer)map.getKey();
                        String[] cope = (String[]) map.getValue();
                        int q = 0;
                        //将小于最小的cope范围的出list
                        while (q < Integer.parseInt(cope[0])) {
                            list.remove(q);
                        }

                        //判断范围内部的点是否合法，循环条件所指定的范围。
                        if (isValidPath(allVs, pageId, cope)) {
                            lists.add(list);
                        }

                    }

                }
            }
        }

        /**
         * 验证这条路径是否是合法的路径
         * @param allVs
         * @param pageId
         * @param cope
         * @return
         */
        public static boolean isValidPath(List<Vertex> allVs, int pageId, String[] cope) {
            //从最小范围开始遍历
            for (int copeIndex = Integer.parseInt(cope[0]);
                 copeIndex <= Integer.parseInt(cope[1]); copeIndex ++) {
                //如果有指定的pageId
                if (allVs.get(copeIndex).getPageId().equals(pageId)) {
                    continue;
                } else if (copeIndex == Integer.parseInt(cope[1])) {
                    return false;
                }
            }

            return true;
        }

    }





























}
