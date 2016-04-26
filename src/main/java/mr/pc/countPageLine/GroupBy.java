package mr.pc.countPageLine;

import Bean.RecordKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by gaoqida on 15-7-2.
 */
public class GroupBy extends WritableComparator {

    public GroupBy() {
        super(RecordKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        RecordKey v1 = (RecordKey) a;
        RecordKey v2 = (RecordKey) b;
        return v1.getSessionId().compareTo(v2.getSessionId());
		/*int compareValue = v1.getUuid().compareTo(v2.getUuid());
		if (compareValue != 0) {
			return compareValue;
		}
		return v1.getSessionTime().compareTo(v2.getSessionTime());*/
    }
}

