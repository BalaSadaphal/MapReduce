package secondarySort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Text, Text, TextIntWritablePair, Text> {

	@Override
	protected void map(Text brand_productName, Text reviewCount,
			Mapper<Text, Text, TextIntWritablePair, Text>.Context context) throws IOException,
			InterruptedException {
		
		String[] words= brand_productName.toString().split("\\|");
		
		String product=words[1];
		String brand=words[0];
		Integer review= Integer.parseInt(reviewCount.toString());
		
		StringBuilder entireRecord=new StringBuilder(brand).append("||").append(product).append("||").append(review);
		
		
		context.write(new TextIntWritablePair(brand,review), new Text(entireRecord.toString()));
	}
}
