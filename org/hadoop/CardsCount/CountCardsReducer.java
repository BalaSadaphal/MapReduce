package org.hadoop.CardsCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCardsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text mapkey, Iterable<IntWritable> valueList,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum=0;
		for(IntWritable value : valueList)
		{
			sum=sum+value.get();
		}
		context.write(mapkey, new IntWritable(sum));
		
	}

}
