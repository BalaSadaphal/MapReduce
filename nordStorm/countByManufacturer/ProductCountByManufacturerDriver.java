package nordStorm.countByManufacturer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProductCountByManufacturerDriver extends Configured implements Tool{

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
		
		job.setMapperClass(ProductCountByManufacturerMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setReducerClass(ProductCountByManufacturerReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		return job.waitForCompletion(true)? 0:1;
		
	}
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ProductCountByManufacturerDriver(), args);
	}

}
