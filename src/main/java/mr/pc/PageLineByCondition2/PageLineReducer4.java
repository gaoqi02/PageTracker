package mr.pc.PageLineByCondition2;

import Bean.Flow;
import Bean.PathKey;
import Bean.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by gaoqida on 15-7-12.
 */
public class PageLineReducer4 extends Reducer<PathKey, Vertex, Text, Text> {


    private static final Pattern SPLIT = Pattern.compile("\\|");

    /**
     * 首先生成一个切片然后再拿这个切片去对比内容
     * 非递归的方法去做处理
     */
    //要处理的点的list--->pageId
    String[] vertexArray;
    //要处理的点与点之间的间隔
    String[] conditionArray;

    Flow flow;

    int maxLength = 0;
    int shorLength = 0;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        try {
            conditionArray = SPLIT.split(configuration.get("condition").trim(), 0);
            vertexArray = SPLIT.split(configuration.get("vertex"), 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<String> vertexes = new ArrayList<String>();
        List<String> rangsList = new ArrayList<String>();

        for (String v : vertexArray) {
            vertexes.add(v);
        }
        for (String rang: conditionArray) {
            String[] tmp = rang.split(",");

             //1下表表示范围内最大的一个元素
            maxLength += Integer.parseInt(tmp[1]);
            //0表示最小的一个元素
            shorLength += Integer.parseInt(tmp[0]);
            rangsList.add(rang);
        }

        flow = new Flow();
        flow.setRangsFlowList(rangsList);
        flow.setVertextFlowList(vertexes);

    }

    @Override
    protected void reduce(PathKey key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
        String startId = flow.getVertextFlowList().get(0);
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

        //这个是条件的size也是我们需要去对切片做遍历的次数
        List<List<Vertex>> flows = new ArrayList<List<Vertex>>();

        //生成所有切片
        for (Vertex vertex: startVs) {

            for (int q = shorLength; q <= maxLength; q++) {
                List<Vertex> list = new ArrayList<Vertex>();
                //将最低点之前的点加入
                for (int i =0; i< q; i++) {
                    if (vertex.getStepId() + i < allVs.size()) {
                        list.add(allVs.get(vertex.getStepId() + i));
                    }
                }

                //切片的长度已经到浏览链表的结尾了。。
                if (vertex.getStepId() + q < allVs.size()) {
                    list.add(allVs.get(vertex.getStepId() + q));
                    flows.add(list);
                }
            }

        }
        List<List<Vertex>> targetList = new ArrayList<List<Vertex>>();
        for (List<Vertex> list : flows) {
            List<Vertex> child = new ArrayList<Vertex>();
            child.add(list.get(0));
            buildPaths(0, list, targetList, 1, child);
        }

        for (List<Vertex> list : targetList) {
            StringBuffer stringBuffer = new StringBuffer();
            for (Vertex vertex: list) {
                stringBuffer.append(vertex.getPageId() + "+");
            }

            context.write(new Text(key.getPathId()),new Text(stringBuffer.toString()));
        }
    }


    /**
     * 生成了路径
     * @param flowStep 计算开始的点，这个点也是指定的顶点
     * @param spills 分片
     * @param targetList 所有合理的分片
     * @param conditionNum 目前触发第几个条件，用来跳出循环
     * @param list 复制当前的切片来看是否合理
     */
    public void buildPaths (int flowStep, List<Vertex> spills, List<List<Vertex>> targetList, int conditionNum, List<Vertex> list) {
        String[] rang = flow.getRangsFlowList().get(conditionNum - 1).split(",");
        String target = flow.getVertextFlowList().get(conditionNum);
        int leftRang = Integer.parseInt(rang[0]);
        int rightRang = Integer.parseInt(rang[1]);

        //将left左边的数加入队列
        for (int i= flowStep + 1; i< flowStep + leftRang; i++) {
            if (i >= spills.size()) {
                break;
            }
            list.add(spills.get(i));
        }

        for (int i= flowStep + leftRang; i<= flowStep + rightRang; i++) {
            //这已经进入队列了
            if (i == flowStep) {
                continue;
            }
            if (i >= spills.size()) {
                break;
            }
            list.add(spills.get(i));

            if (spills.get(i).getPageId().equals(target)) {
                conditionNum ++;
                if (conditionNum != flow.getRangsFlowList().size()) {
                    buildPaths(i, spills, targetList, conditionNum, list);
                }
            }
        }

        if (conditionNum == flow.getRangsFlowList().size()) {
            List<Vertex> childPath = new ArrayList<Vertex>();
            for (Vertex vertex: list ) {
                childPath.add(vertex);
            }
            targetList.add(childPath);
        }
    }







}
