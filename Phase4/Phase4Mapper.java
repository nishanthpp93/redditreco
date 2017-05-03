package phase4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Phase4Mapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] val = value.toString().split("\\t");
		String primarySubr = val[0].trim();
		String[] intersections = val[1].split(",");
		
		context.write(new Text(primarySubr), new Text(intersections[0].trim()));
		
		for (int i = 1; i < intersections.length; i++) {
			String[] subrCount = intersections[i].split("\\s+");
			context.write(new Text(subrCount[0]), new Text(primarySubr + " " + subrCount[1].trim() + " " + intersections[0].trim()));
		}
	}
}
