package org.hadoop.CardsCount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(Driver.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		Path outputDir =new Path(args[1]);
		 FileSystem hdfs = FileSystem.get(getConf());
		    if (hdfs.exists(outputDir))
		      hdfs.delete(outputDir, true);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(SeggregateCardsMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(CountCardsReducer.class);;
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		return job.waitForCompletion(true) ? 0: 1;
		
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Driver(), args);
	}
	
}
