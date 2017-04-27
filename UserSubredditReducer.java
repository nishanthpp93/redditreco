import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class UserSubredditReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder subreddits = new StringBuilder();
		for (Text value : values) {
			subreddits.append(value).append(",");
		}
		
		context.write(key,  new Text(subreddits.toString().substring(0, subreddits.toString().length() - 1)));
	}
}