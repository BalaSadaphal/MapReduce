package nordStorm.countByManufacturer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductCountByManufacturerMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	protected void map(LongWritable lineoffset, Text line, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
	
		String[] words= line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		if(!(words[0].equalsIgnoreCase("id")))
			context.write(new Text(words[1]),new LongWritable(1)) ;
		
		
	}
}
