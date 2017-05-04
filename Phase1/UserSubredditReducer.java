import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserSubredditReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> uniqueSubreddits = new HashSet<>();
		StringBuilder subreddits = new StringBuilder();
		for (Text value : values) {
			if (!uniqueSubreddits.contains(value.toString())) {
				subreddits.append(value.toString()).append(",");
				uniqueSubreddits.add(value.toString());
			}
		}
		context.write(key,  new Text(subreddits.toString().substring(0, subreddits.toString().length() - 1)));
	}
}
