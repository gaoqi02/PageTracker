package Bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 只有Key值才实现这个接口。。
 * Created by gaoqida on 15-7-2.
 */
public class Vertex implements Writable {

    private int userid;
    private String sessionId;
    private int dealid;
    private double amount;
    private String spm;
    private String preSpm;
    private long timeStamp;

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
        dataOutput.writeInt(userid);
        dataOutput.writeUTF(sessionId);
        dataOutput.writeInt(dealid);
        dataOutput.writeDouble(amount);
        dataOutput.writeInt(pathId);
        dataOutput.writeInt(stepId);
        dataOutput.writeLong(timeStamp);
        dataOutput.writeUTF(pageId);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userid = dataInput.readInt();
        this.sessionId = dataInput.readUTF();
        this.dealid = dataInput.readInt();
        this.amount = dataInput.readDouble();
        this.pathId = dataInput.readInt();
        this.stepId = dataInput.readInt();
        this.timeStamp = dataInput.readLong();
        this.pageId = dataInput.readUTF();

    }

    public Vertex(int userid, String sessionId, int dealid, double amount, String spm, String preSpm,String pageId, long timeStamp) {
        this.userid = userid;
        this.sessionId = sessionId;
        this.dealid = dealid;
        this.amount = amount;
        this.spm = spm;
        this.preSpm = preSpm;
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


    @Override
    public String toString() {
        return userid+":"+sessionId+":"+pathId+":"+stepId+":"+pageId+":"+timeStamp;
    }

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public int getDealid() {
        return dealid;
    }

    public void setDealid(int dealid) {
        this.dealid = dealid;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getSpm() {
        return spm;
    }

    public void setSpm(String spm) {
        this.spm = spm;
    }

    public String getPreSpm() {
        return preSpm;
    }

    public void setPreSpm(String preSpm) {
        this.preSpm = preSpm;
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
