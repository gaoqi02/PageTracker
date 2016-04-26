package MobileBean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 移动端的对象
 * Page的名称,Page表明的key,Page的详细信息，时间戳，SessionId,经度,纬度,设备Id
 * 其中设备id和sessionId可以单独唯一确定一个当前用户，不会用重复的。
 * Created by gaoqida on 15-8-2.
 */
public class Vertex implements Writable {

    private String sessionId;
    /**
     * key和detail表示一个页面的信息
     */
    private String detail;
    private int key;
    /**
     * 时间戳
     */
    private long timeStamp;
    /**
     * 设备Id
     */
    private String deviceId;

    private List<Vertex> vertexList;

    private int stepId;
    private int pathId;
    private String pageId; // we count meanTime per page .

    public int getPathId() {
        return pathId;
    }

    public void setPathId(int pathId) {
        this.pathId = pathId;
    }

    public Vertex() {
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(key);
        dataOutput.writeUTF(detail);
        dataOutput.writeUTF(deviceId);
        dataOutput.writeUTF(sessionId);
        dataOutput.writeInt(pathId);
        dataOutput.writeInt(stepId);
        dataOutput.writeLong(timeStamp);
        dataOutput.writeUTF(pageId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.key = dataInput.readInt();
        this.detail = dataInput.readUTF();
        this.deviceId = dataInput.readUTF();
        this.sessionId = dataInput.readUTF();
        this.pathId = dataInput.readInt();
        this.stepId = dataInput.readInt();
        this.timeStamp = dataInput.readLong();
        this.pageId = dataInput.readUTF();

    }

    public Vertex(String sessionId,String pageId, long timeStamp) {
        this.sessionId = sessionId;
        this.timeStamp = timeStamp;
        this.pageId = pageId;
        this.vertexList = new LinkedList<Vertex>();
    }

    public Vertex(String sessionId, int pathId, int stepId, long timeStamp){
        this.sessionId = sessionId;
        this.pathId = pathId;
        this.stepId = stepId;
        this.timeStamp = timeStamp;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public List<Vertex> getVertexList() {
        return vertexList;
    }

    public void setVertexList(List<Vertex> vertexList) {
        this.vertexList = vertexList;
    }

    public int getStepId() {
        return stepId;
    }

    public void setStepId(int stepId) {
        this.stepId = stepId;
    }
}
