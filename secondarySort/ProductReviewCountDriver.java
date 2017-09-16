package secondarySort;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProductReviewCountDriver extends Configured implements Tool{
	
	public int run(String[] arg0) throws Exception {
			Job job1=Job.getInstance(getConf());
			job1.setJarByClass(getClass());
			
			FileInputFormat.setInputPaths(job1, arg0[0]);
			Path outPutDir2 = new Path(arg0[1]);
			FileSystem fs1=FileSystem.get(getConf());
			if(fs1.exists(outPutDir2)){
				fs1.delete(outPutDir2, true);
			}
			
			FileOutputFormat.setOutputPath(job1, outPutDir2);
			
			job1.setMapperClass(Mapper2.class);
			job1.setInputFormatClass(KeyValueTextInputFormat.class);
			job1.setMapOutputKeyClass(TextIntWritablePair.class);
			job1.setOutputValueClass(Text.class);
			
			job1.setPartitionerClass(BrandPartitioner.class);
			job1.setGroupingComparatorClass(BrandGroupComparator.class);
			job1.setSortComparatorClass(BrandReviewSortComparator.class);
				
			job1.setNumReduceTasks(2);
			job1.setReducerClass(Reducer2.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			return job1.waitForCompletion(true)? 0:1;
			
				
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ProductReviewCountDriver(), args);
	}

}
