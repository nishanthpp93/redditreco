package phase4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase4Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int individualCount = 0;
		List<String> secondarySubreddits = new ArrayList<>();
		List<Integer> intersectionCount = new ArrayList<>();
		List<Integer> secondarySubredditCount = new ArrayList<>();
		
		for (Text value : values) {
			String[] intersections = value.toString().split("\\s+");
			if (intersections.length > 1) {
				int index = 0;
				secondarySubreddits.add(intersections[index++].trim());
				intersectionCount.add(Integer.parseInt(intersections[index++].trim()));
				secondarySubredditCount.add(Integer.parseInt(intersections[index++].trim()));
			} else {
				individualCount = Integer.parseInt(intersections[0].trim());
			}
		}
		int index = 0;
		
		while (index < secondarySubreddits.size()) {
			double score = ((double) intersectionCount.get(index)) / ((double) (individualCount + secondarySubredditCount.get(index) - intersectionCount.get(index))); 
			context.write(new Text(secondarySubreddits.get(index) + "|" + key), new DoubleWritable(score));
			index++;
		}
	}
}
