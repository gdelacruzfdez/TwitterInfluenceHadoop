package partitioner;

import model.CompositeTweetKey;
import model.Tweet;

import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends
		Partitioner<CompositeTweetKey, Tweet> {

	@Override
	public int getPartition(CompositeTweetKey key, Tweet value,
			int numPartitions) {
		return key.getUser().hashCode() % numPartitions;
	}

}
