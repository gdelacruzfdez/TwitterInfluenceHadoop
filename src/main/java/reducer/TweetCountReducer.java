package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import model.CompositeTweetKey;
import model.Tweet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TweetCountReducer extends
		Reducer<CompositeTweetKey, Tweet, NullWritable, Text> {

	public static final int N = 20;

	private Text outputValue = new Text();

	@Override
	protected void reduce(
			CompositeTweetKey key,
			Iterable<Tweet> values,
			Reducer<CompositeTweetKey, Tweet, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		List<Tweet> lastN = new ArrayList<Tweet>();
		int totalTweets = 0;

		for (Tweet t : values) {
			if (lastN.size() < N) {
				lastN.add(new Tweet(t));
			}
			totalTweets++;
		}

		double volumeMomentum = calculateVolumeMomentum(lastN);
		double popularityMomentum = calculatePopularityMomentum(lastN);

		StringBuffer sb = new StringBuffer();
		sb.append(key.getUser());
		sb.append(",");
		sb.append(totalTweets);
		sb.append(",");
		sb.append(volumeMomentum);
		sb.append(",");
		sb.append(popularityMomentum);
		outputValue.set(sb.toString());
		context.write(null, outputValue);
	}

	private double calculateVolumeMomentum(List<Tweet> lastN) {
		if (lastN.size() > 1) {
			Tweet tN = lastN.get(0);
			Tweet t0 = lastN.get(lastN.size() - 1);
			Tweet tN2 = lastN.get((lastN.size() - 1) / 2);
			if (tN.getTimestamp() - t0.getTimestamp() != 0) {
				return ((tN.getTimestamp() - t0.getTimestamp()) - (tN
						.getTimestamp() - tN2.getTimestamp()))
						/ (tN.getTimestamp() - t0.getTimestamp());
			}
		}
		return 0.0;
	}

	private double calculatePopularityMomentum(List<Tweet> lastN) {
		if (lastN.size() > 1) {
			int N2 = (lastN.size() - 1) / 2;
			int retweetsUntilN2 = 0;
			int totalRetweets = 0;
			for (int i = 0; i < lastN.size(); i++) {
				totalRetweets += lastN.get(i).getNumRetweets();
				if (i <= N2) {
					retweetsUntilN2 += lastN.get(i).getNumRetweets();
				}
			}
			if (totalRetweets > 0) {
				return (double) retweetsUntilN2 / totalRetweets;
			}
		}
		return 0;

	}
}
