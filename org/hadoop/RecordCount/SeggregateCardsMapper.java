package org.hadoop.RecordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SeggregateCardsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line= value.toString();
		
		String outKey= line.substring(0,line.indexOf("|"));
		
		context.write(new Text(outKey), new IntWritable(1));		
		
	}

}
