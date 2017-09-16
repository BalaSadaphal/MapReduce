package org.hadoop.CardsCount;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

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
		
		String[] str=line.split("\\|");
					
		System.out.println(Arrays.toString(str));
		
		StringBuilder tempKey=new StringBuilder();
		
		tempKey.append(str[1]);
		
		System.out.println(tempKey.toString());
		//String outKey= line.substring(0,line.indexOf("|"));
		//context.getCounter(tempKey.toString());
		context.write(new Text(tempKey.toString()), new IntWritable(1));		
		
	}

}
