package sortcomparator;

import model.CompositeTweetKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

	public GroupingComparator() {
		super(CompositeTweetKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeTweetKey k1 = (CompositeTweetKey) a;
		CompositeTweetKey k2 = (CompositeTweetKey) b;

		return k1.getUser().compareTo(k2.getUser());
	}

}
