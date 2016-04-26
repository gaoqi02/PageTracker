package Bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by gaoqi on 2015/7/4.
 */
public class Page implements Writable{

    private int id;
    private int count;
    private Long timeStamp;
    private String uuid;
    private String sessionId;

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.count = dataInput.readInt();
        this.timeStamp = dataInput.readLong();
        this.uuid = dataInput.readUTF();
        this.sessionId = dataInput.readUTF();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(count);
        dataOutput.writeLong(timeStamp);
        dataOutput.writeUTF(uuid);
        dataOutput.writeUTF(sessionId);
    }
}
