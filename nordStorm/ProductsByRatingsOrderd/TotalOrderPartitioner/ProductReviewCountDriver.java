package nordStorm.ProductsByRatingsOrderd.TotalOrderPartitioner;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductReviewCountDriver extends Configured implements Tool{
	
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, arg0[0]);
		Path outPutDir = new Path(arg0[1]);
		FileSystem fs=FileSystem.get(getConf());
		if(fs.exists(outPutDir)){
			fs.delete(outPutDir, true);
		}
		
		FileOutputFormat.setOutputPath(job, outPutDir);
		
		job.setMapperClass(ProductReviewCountMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setReducerClass(ProductReviewCountReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		Boolean status= job.waitForCompletion(true);
				
		if(status==true)
		{
			Job job1=Job.getInstance(getConf());
			job1.setJarByClass(getClass());
			
			FileInputFormat.setInputPaths(job1, arg0[1]);
			Path outPutDir2 = new Path("MR2_"+arg0[1]);
			FileSystem fs1=FileSystem.get(getConf());
			if(fs1.exists(outPutDir2)){
				fs1.delete(outPutDir2, true);
			}
			
			FileOutputFormat.setOutputPath(job1, outPutDir2);
			
			job1.setMapperClass(SecondMapper.class);
			job1.setInputFormatClass(KeyValueTextInputFormat.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			job1.setPartitionerClass(TotalOrderPartitioner.class);
			InputSampler.Sampler<Text,Text> sampler = new InputSampler.RandomSampler<Text,Text>(0.1,400,2);
			//InputSampler.Sampler<Text,Text> sampler = new InputSampler.IntervalSampler<Text,Text>(20);
			InputSampler.writePartitionFile(job1, sampler);
					
			// Add partition file to Distributed Cache
					
			String partitionFile = TotalOrderPartitioner.getPartitionFile(getConf());
			URI partitionUri = new URI(partitionFile);
			job1.addCacheFile(partitionUri);
			
			job1.setNumReduceTasks(3);
			job1.setReducerClass(Reducer.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			return job1.waitForCompletion(true)? 0:1;
			
			
		}
			
		else
			return 1;
		
		
		
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ProductReviewCountDriver(), args);
	}

}
