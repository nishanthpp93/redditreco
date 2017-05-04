import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SubredditCount extends Configured implements Tool {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    Mapper.Context context) throws IOException, InterruptedException {
      String[] line = (value.toString()).split(",");
      if (!(line[0].equals("[deleted]"))){
        for (int i = 1; i<line.length; i++){
          context.write(new Text(line[i]), one);
          for (int j = i+1; j<line.length; j++){
            if (line[i].toLowerCase().compareTo(line[j].toLowerCase()) < 0){
	      context.write(new Text(line[i]+" "+line[j]), one);
	    }
	    else {
              context.write(new Text(line[j]+" "+line[i]), one);
	    }
          }
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      long sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      if (key.toString().contains(" ")){
        String[] keynew = (key.toString()).split("\\s+");
        context.write(new Text(keynew[0]), new Text(keynew[1]+" "+String.valueOf(sum)));
      }
      else{
        context.write(key, new Text(String.valueOf(sum)));
      }
    }
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new SubredditCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    Configuration conf = getConf();
    Job job = new Job(conf, this.getClass().toString());

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setJobName("SubredditCount");
    job.setJarByClass(SubredditCount.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

}
