package mr.pc.CommonFuction;

import Bean.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 根据sessionId和pathId来放一起
 * Created by gaoqida on 15-7-2.
 */
public class GroupBySessionIdPathId extends WritableComparator {

    public GroupBySessionIdPathId() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        RecordKey v1 = (RecordKey) a;
//        RecordKey v2 = (RecordKey) b;
//
//        if (v1.getSessionId().equals(v2.getSessionId())){
//
//            if (v1.getPathId() == v2.getPathId()) {
//                return 0;
//            } else {
//                return v1.getPathId()>v2.getPathId() ? -1: 1;
//            }
//        } else {
//            return v1.getPathId()>v2.getPathId() ? -1: 1;
//        }
        PathKey v1 = (PathKey) a;
        PathKey v2 = (PathKey) b;
        return (v1.getPathId()).compareTo(v2.getPathId());
		/*int compareValue = v1.getUuid().compareTo(v2.getUuid());
		if (compareValue != 0) {
			return compareValue;
		}
		return v1.getSessionTime().compareTo(v2.getSessionTime());*/
    }
}

