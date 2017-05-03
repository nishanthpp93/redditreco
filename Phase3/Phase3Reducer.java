package phase3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Phase3Reducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder subredditIntersection = new StringBuilder();
		String individualCount = ""; 
		for (Text value : values) {
			if (value.toString().contains(" ")) {
				subredditIntersection.append(",").append(value);
			} else {
				individualCount = value.toString();
			}
		}
		context.write(key,  new Text(individualCount + subredditIntersection.toString()));
	}
}
