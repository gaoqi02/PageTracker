package Bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by gaoqida on 15-7-19.
 */
public class PathKey implements WritableComparable<PathKey> {

    private String pathId;
    private long timestamp;

    public int compareTo(PathKey o) {

        if (this.pathId.equals(o.getPathId())){
            return this.timestamp>o.getTimestamp() ? 1:-1;
        } else {
            return this.pathId.compareTo(o.getPathId());
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(pathId);
        dataOutput.writeLong(timestamp);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.pathId = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }
//
//    @Override
//    public boolean equals(Object right) {
//        if (right instanceof RecordKey) {
//            PathKey r = (PathKey) right;
//            return this.pathId.equals(r.getPathId());
//        } else {
//            return false;
//        }
//    }

    public String getPathId() {
        return pathId;
    }

    public void setPathId(String pathId) {
        this.pathId = pathId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
