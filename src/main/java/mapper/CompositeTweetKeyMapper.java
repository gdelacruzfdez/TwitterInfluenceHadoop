package mapper;

import java.io.IOException;

import model.CompositeTweetKey;
import model.Tweet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CompositeTweetKeyMapper extends
		Mapper<LongWritable, Tweet, CompositeTweetKey, Tweet> {

	private CompositeTweetKey newKey = new CompositeTweetKey();

	@Override
	protected void map(
			LongWritable key,
			Tweet value,
			Mapper<LongWritable, Tweet, CompositeTweetKey, Tweet>.Context context)
			throws IOException, InterruptedException {

		newKey.setUser(value.getUser());
		newKey.setTimestamp(value.getTimestamp());
		context.write(newKey, value);
	}

}
