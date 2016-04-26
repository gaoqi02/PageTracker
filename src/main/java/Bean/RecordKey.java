package Bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by gaoqi on 2015/6/29.
 */
public class RecordKey implements WritableComparable<RecordKey> {

    //生成路径的时候需要
    private int userid;
    private String sessionId;
    private long timeStamp;



    public int compareTo(RecordKey o) {
        return this.timeStamp > o.getTimeStamp() ? 1 : -1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(userid);
        dataOutput.writeUTF(sessionId);
        dataOutput.writeLong(timeStamp);
    }

    @Override
    public boolean equals(Object right) {
        if (right instanceof RecordKey) {
            RecordKey r = (RecordKey) right;
            return this.sessionId.equals(r.getSessionId());
        } else {
            return false;
        }
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.userid = dataInput.readInt();
        this.sessionId = dataInput.readUTF();
        this.timeStamp = dataInput.readLong();
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

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
