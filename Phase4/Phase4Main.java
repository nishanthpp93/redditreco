package phase4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Phase4Main {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: Phase4Main <input path> <output path>");
			System.exit(-1);
		}
    
		Job job = new Job();
		job.setJarByClass(Phase4Main.class);
		job.setJobName("Phase4Main");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Phase4Mapper.class);
		job.setReducerClass(Phase4Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
