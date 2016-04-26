package mr.pc.PageLineByCondition2;

import Bean.PathKey;
import Bean.Vertex;
import Bean.VertexCondition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by gaoqida on 15-7-12.
 */
public class PageLineReducer2 extends Reducer<PathKey, Vertex, NullWritable, Text> {


    private static final Pattern SPLIT = Pattern.compile("\\|");

    /**
     * 首先生成一个切片然后再拿这个切片去对比内容
     * 非递归的方法去做处理
     */
    //要处理的点的list--->pageId
    String[] vertexArray;
    //要处理的点与点之间的间隔
    String[] conditionArray;
    //两个页面之间的间距  []0是小  1是大的
    List<VertexCondition> vertexConditionList;

    //这里用一个hashSet来过滤切片中的是否包含这些点。可以用重复，这只是看这些点是否符合需求，如果匹配的size<这个hashSet的话就证不符合
    HashSet<String> vertexSet;

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

        vertexConditionList = new ArrayList<VertexCondition>();

        vertexSet = new HashSet<String>();

        for (String s: vertexArray) {
            String pageId = s;

            vertexSet.add(pageId);
            String[] tmp = conditionArray[vertexConditionList.size()].split(",");
            //1下表表示范围内最大的一个元素
            maxLength += Integer.parseInt(tmp[1]);
            //0表示最小的一个元素
            shorLength += Integer.parseInt(tmp[0]);
            //这个map就如矩阵一样  v ---> 到下一个v的范围
            VertexCondition vertexCondition = new VertexCondition();
            vertexCondition.setPageId(pageId);
            vertexCondition.setCondition(tmp);
            vertexConditionList.add(vertexCondition);
        }
    }

    @Override
    protected void reduce(PathKey key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {

        //记录了起点的step_id
        List<Integer> startVList = new ArrayList<Integer>();
        List<Vertex> allVs = new ArrayList<Vertex>();

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
            startVList.add(index);
            index ++;

        }

        //承载着所有合理的路径
        List<List<Vertex>> lists = new ArrayList<List<Vertex>>();

        try {

            for (Integer i : startVList) {

                for (int end = shorLength; end <= maxLength; end++) {
                    //这个是对应的切片
                    List<Vertex> list = new ArrayList<Vertex>();

                    //去看这些点是否在这个map中出现，如果flag小于等于这个map的size就表明指定的点并没有完全出现
                    int flag = 0;
                    for (int count = i; count <= end; count++) {
                        list.add(allVs.get(count));

                        if (vertexSet.contains(allVs.get(count).getPageId())) {
                            flag++;
                        }

                    }

                    //判断所有的点是否全部包含指定的点。
                    if (flag < vertexConditionList.size()) {
                        continue;
                    }

                    flag = 0;
                    //循环所有的条件看是否OK。。
                    for (int q = 0; q< vertexConditionList.size() - 1; q++) {
                        String pageId = vertexConditionList.get(q+1).getPageId();
                        String[] cope = vertexConditionList.get(q).getCondition();
//                        int q = 0;
//                        //将小于最小的cope范围的出list
//                        while (q < Integer.parseInt(cope[0])) {
//                            list.remove(q);
//                            q++;
//                        }

                        //判断范围内部的点是否合法，循环条件所指定的范围。
                        if (isValidPath(list, pageId, cope)) {
                            flag ++;
                        } else {
                            //只要有一个条件未满足就跳出
                            break;
                        }

                    }

                    if (flag >= vertexConditionList.size() - 1) {
                        lists.add(list);
                    }
                }
            }

            for (List<Vertex> list: lists) {
                for (Vertex v : list) {
                    Vertex vertex = new Vertex();
                    vertex.setPathId(v.getPathId());
                    vertex.setPageId(v.getPageId());
                    vertex.setSessionId(v.getSessionId());

                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("vertex", vertex);

                    context.write(null, new Text(jsonObject.toString()));
                }
            }
        } catch (Exception e) {
            System.out.print("@@"+e.getLocalizedMessage());
            return;
        }
    }

    /**
     * 验证这条路径是否是合法的路径
     * @param allVs 切片的所有点
     * @param pageId
     * @param cope
     * @return
     */
    public static boolean isValidPath(List<Vertex> allVs, String pageId, String[] cope) {
        //从最小范围开始遍历
        for (int copeIndex = Integer.parseInt(cope[0]);
             copeIndex <= Integer.parseInt(cope[1]); copeIndex ++) {
            //如果有指定的pageId
            if (allVs.get(copeIndex).getPageId().equals(pageId)) {
                return true;
            } else if (copeIndex == Integer.parseInt(cope[1])) {
                continue;
            }
        }

        return false;
    }



}
