package inputformat;

import java.io.IOException;

import model.CompositeTweetKey;
import model.Tweet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TweetInputFormat extends FileInputFormat<CompositeTweetKey, Tweet> {

	@Override
	public RecordReader<CompositeTweetKey, Tweet> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new TweetReader();
	}

	static class TweetReader extends RecordReader<CompositeTweetKey, Tweet> {

		private CompositeTweetKey key = new CompositeTweetKey(); // user
		private Tweet value;
		private LineReader in;
		private long start;
		private long end;
		private long currentPos;
		private Text line = new Text();

		private ObjectMapper mapper = new ObjectMapper();

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) split;
			Configuration conf = context.getConfiguration();
			Path path = fileSplit.getPath();
			FSDataInputStream is = path.getFileSystem(conf).open(path);
			in = new LineReader(is, conf);

			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			is.seek(start);
			if (start != 0) {
				start += in.readLine(new Text(), 0,
						(int) Math.min(Integer.MAX_VALUE, end - start));
			}
			currentPos = start;

			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
					false);

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (currentPos > end) {
				return false;
			}
			currentPos += in.readLine(line);

			if (line.getLength() == 0) {
				return false;
			}
			value = mapper.readValue(line.toString(), Tweet.class);
			key.setUser(value.getUser());
			key.setTimestamp(value.getTimestamp());

			return true;

		}

		@Override
		public CompositeTweetKey getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Tweet getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() throws IOException {
			in.close();

		}

	}

}
