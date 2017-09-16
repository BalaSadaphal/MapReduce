package nordStorm.productRatings.SecondarySort;

import java.io.IOException;

import nordStorm.productRatings.counters.ProductReviewCountDriver.RatingsTracker;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<TextIntWritablePair, Text, LongWritable, Text> {
	
	int count=1;
	@Override
	protected void reduce(TextIntWritablePair key, Iterable<Text> entireLines,
			Reducer<TextIntWritablePair, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		
				
		for(Text line:entireLines)
			{
				if (count <= 10)
				context.write(new LongWritable(count), line);
				count++;
				
			}
					
	}

}
