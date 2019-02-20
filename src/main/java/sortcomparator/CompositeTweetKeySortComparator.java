package sortcomparator;

import model.CompositeTweetKey;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeTweetKeySortComparator extends WritableComparator {

	public CompositeTweetKeySortComparator() {
		super(CompositeTweetKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeTweetKey k1 = (CompositeTweetKey) a;
		CompositeTweetKey k2 = (CompositeTweetKey) b;

		int result = k1.getUser().compareTo(k2.getUser());
		if (result == 0) {
			return (int) -(k1.getTimestamp() - k2.getTimestamp());
		}
		return result;
	}

}
