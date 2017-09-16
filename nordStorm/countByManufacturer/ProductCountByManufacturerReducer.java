package nordStorm.countByManufacturer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductCountByManufacturerReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {
	
	@Override
	protected void reduce(Text manufacturer, Iterable<LongWritable> countList,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		int total=0;
		for(LongWritable count:countList){
			
				total=total+1;			
		}
		context.write(manufacturer, new LongWritable(total));
		
		
	}

}
