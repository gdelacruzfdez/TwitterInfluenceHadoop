package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import deserializer.TweetDeserializer;

@JsonDeserialize(using = TweetDeserializer.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements Writable {

	private long id;
	private String text;
	private double timestamp;
	private String user;
	private int numRetweets;

	public Tweet(long id, String text, double timestamp, String user,
			int numRetweets) {
		super();
		this.id = id;
		this.text = text;
		this.timestamp = timestamp;
		this.user = user;
		this.numRetweets = numRetweets;
	}

	public Tweet() {

	}

	public Tweet(Tweet t) {
		this.id = t.id;
		this.text = t.text;
		this.timestamp = t.timestamp;
		this.user = t.user;
		this.numRetweets = t.numRetweets;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeUTF(text);
		out.writeDouble(timestamp);
		out.writeUTF(user);
		out.writeInt(numRetweets);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		this.text = in.readUTF();
		this.timestamp = in.readLong();
		this.user = in.readUTF();
		this.numRetweets = in.readInt();

	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public int getNumRetweets() {
		return numRetweets;
	}

	public void setNumRetweets(int numRetweets) {
		this.numRetweets = numRetweets;
	}

}
