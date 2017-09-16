package nordStorm.productRatings.MultipleInputFilesAndReducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductReviewCountReducer extends
		Reducer<Text, Text, Text, LongWritable> {
	
	@Override
	protected void reduce(Text product, Iterable<Text> ratings,
			Reducer<Text, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		int count=0;
		for(Text rating:ratings){
			if(rating.equals("0")){		
			}else
			{
				count=count+1;
			}
				
		}
		context.write(product, new LongWritable(count));
		
		
	}

}
