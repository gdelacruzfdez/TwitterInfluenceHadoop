package jobs;

import inputformat.TweetInputFormat;
import model.CompositeTweetKey;
import model.Tweet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import partitioner.SecondarySortPartitioner;
import reducer.TweetCountReducer;
import sortcomparator.CompositeTweetKeySortComparator;
import sortcomparator.GroupingComparator;

public class TwitterInfluenceJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "TwitterInfluence");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());

		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		// job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(TweetCountReducer.class);
		job.setInputFormatClass(TweetInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setSortComparatorClass(CompositeTweetKeySortComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);

		job.setMapOutputKeyClass(CompositeTweetKey.class);
		job.setMapOutputValueClass(Tweet.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) {
		int result;
		try {
			result = ToolRunner.run(new Configuration(),
					new TwitterInfluenceJob(), args);
			System.exit(result);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
