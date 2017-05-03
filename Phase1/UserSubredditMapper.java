import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;

public class UserSubredditMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String author;
		String subreddit;
		if (!value.toString().trim().equals("")) {
			JSONObject obj = new JSONObject(value.toString());
			subreddit = obj.getString("subreddit");
			author = obj.getString("author");
			context.write(new Text(author), new Text(subreddit));
		}
	}
}
