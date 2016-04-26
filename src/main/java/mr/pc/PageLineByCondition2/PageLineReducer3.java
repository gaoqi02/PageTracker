package mr.pc.PageLineByCondition2;

import Bean.PathKey;
import Bean.Vertex;
import Bean.VertexCondition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.regex.Pattern;

/**
 * Created by gaoqida on 15-7-12.
 */
public class PageLineReducer3 extends Reducer<PathKey, Vertex, Text, Text> {


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

        String startId = vertexArray[0];
        //存放所有的点
        List<Vertex> allVs = new ArrayList<Vertex>();
        //存放起点的位置
        List<Vertex> startVs = new ArrayList<Vertex>();
        //存所有vertex
        for (Vertex vertex : values) {
            Vertex tmp = new Vertex();
            tmp.setPageId(vertex.getPageId());
            tmp.setSessionId(vertex.getSessionId());
            tmp.setStepId(vertex.getStepId());
            tmp.setTimeStamp(vertex.getTimeStamp());
            tmp.setPageId(vertex.getPageId());

            if (vertex.getPageId().equals(startId)) {
                startVs.add(tmp);
            }
            allVs.add(tmp);
        }

        List<List<Vertex>> list = new ArrayList<List<Vertex>>();
        Stack<Vertex> stack = new Stack<Vertex>();

        for (Vertex startVertex : startVs) {
            stack.push(startVertex);
            isValidPath1(stack, 0, allVs, list,startVertex);

        }

        JSONObject jsonObject = new JSONObject();

        for (List<Vertex> list1: list) {
            for (Vertex vertex: list1) {
                try {
                    jsonObject.put("vertex",vertex);
                    context.write(new Text(key.getPathId().split(":")[1]), new Text(jsonObject.toString()));
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            }
        }

    }

    public void isValidPath1(Stack<Vertex> stack, int conditionNum, List<Vertex> allVs, List<List<Vertex>> list,Vertex root) {

        if (conditionNum == vertexConditionList.size() - 1) {
            //最终生成一条有序合理的链。new List防止后续再插入stack....
            List<Vertex> list1 = new ArrayList<Vertex>();
            for (Vertex vertex: stack) {
                list1.add(vertex);
            }
            list.add(list1);
            return;
        }
        String[] cope = vertexConditionList.get(conditionNum).getCondition();

        try {
            for (int i=1; i< Integer.parseInt(cope[0]); i++) {
                stack.push(allVs.get(stack.peek().getStepId() + i));
            }

            for (int i=Integer.parseInt(cope[0]); i<= Integer.parseInt(cope[1]); i++) {

                if(i == 0) {continue;}

                if (root.getStepId() + i >= allVs.size()) {
                    break;
                }

                if (allVs.get(root.getStepId() + i).getPageId().equals(vertexArray[conditionNum + 1])) {
                    stack.push(allVs.get(root.getStepId() + i));
                    conditionNum ++;
                    isValidPath1(stack, conditionNum, allVs, list, stack.peek());
                } else {
                    stack.push(allVs.get(root.getStepId() + i));
                }
            }
        } catch (Exception e) {
            System.out.print("@@"+e.getMessage());
            return;
        }

    }





}
