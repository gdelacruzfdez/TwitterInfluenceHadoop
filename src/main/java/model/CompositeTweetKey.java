package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CompositeTweetKey implements Writable,
		WritableComparable<CompositeTweetKey> {

	String user;
	double timestamp;

	public CompositeTweetKey(String user, double timestamp) {
		super();
		this.user = user;
		this.timestamp = timestamp;
	}

	public CompositeTweetKey() {

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(user);
		out.writeDouble(timestamp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.user = in.readUTF();
		this.timestamp = in.readLong();

	}

	@Override
	public int compareTo(CompositeTweetKey key) {
		int result = user.compareTo(key.getUser());
		if (0 == result) {
			result = (int) (timestamp - key.getTimestamp());
		}
		return result;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	}

}
