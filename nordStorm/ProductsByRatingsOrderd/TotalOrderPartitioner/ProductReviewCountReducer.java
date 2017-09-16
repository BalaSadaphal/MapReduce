package nordStorm.ProductsByRatingsOrderd.TotalOrderPartitioner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductReviewCountReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {
	
	@Override
	protected void reduce(Text product, Iterable<LongWritable> ratings,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		int count=0;
		for(LongWritable rating:ratings){
			if(rating.equals(0)){		
			}else
			{
				count=count+1;
			}
				
		}
		context.write(product, new LongWritable(count));
		
		
	}

}
