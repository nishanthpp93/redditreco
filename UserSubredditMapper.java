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
		String[] itr = (value.toString()).split("\\n");
		for (int i = 0; i < itr.length; i++) {
			JSONObject obj = new JSONObject(itr[i]);
			subreddit = obj.getString("subreddit");
			author = obj.getString("author");
			context.write(new Text(author), new Text(subreddit));
		}
	}
}