package nordStorm.productRatings.MultipleInputFilesAndReducers;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductReviewCountDriver extends Configured implements Tool{
	public enum RatingsTracker {
		 
		Empty_String,
		Single_Character,
		Non_Integer_String,
		Integer_String
		 
		}

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
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(3);
		job.setReducerClass(ProductReviewCountReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		/*
		//Total Order Partitioner
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<Text,Text> sampler = new InputSampler.RandomSampler<Text,Text>(0.1,10000,10);
		InputSampler.writePartitionFile(job, sampler);
				
		// Add partition file to Distributed Cache
				
		String partitionFile = TotalOrderPartitioner.getPartitionFile(getConf());
		URI partitionUri = new URI(partitionFile);
		job.addCacheFile(partitionUri);
		*/
		
		Boolean status= job.waitForCompletion(true);
		
		Counters allCounters = job.getCounters();
		
		for(CounterGroup group: allCounters)
		{
			System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
			 
			System.out.println(" number of counters in this group: " + group.size());
			 
			for (org.apache.hadoop.mapreduce.Counter counter : group) {
			 
			System.out.println(" - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
			 
			}
		}
		
		
		if(status==true)
			return 0;
		else
			return 1;
		
		
		
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ProductReviewCountDriver(), args);
	}

}
