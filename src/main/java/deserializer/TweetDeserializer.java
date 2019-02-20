package deserializer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import model.Tweet;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;


public class TweetDeserializer extends StdDeserializer<Tweet> {

	SimpleDateFormat format = new SimpleDateFormat(
			"E MMM dd HH:mm:ss +0000 yyyy");

	public TweetDeserializer() {
		this(null);
	}

	public TweetDeserializer(Class<?> vc) {
		super(vc);
	}

	@Override
	public Tweet deserialize(JsonParser jp, DeserializationContext ctxt)
			throws IOException, JsonProcessingException {
		JsonNode node = jp.getCodec().readTree(jp);
		long id = node.get("id").asLong();
		String text = node.get("text").asText();
		String user = node.get("user").get("screen_name").asText();
		int numRetweets = node.get("retweet_count").asInt();

		String createdAtStr = node.get("created_at").asText();
		long timestamp = 0;
		try {
			timestamp = format.parse(createdAtStr).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return new Tweet(id, text, timestamp, user, numRetweets);
	}

}
