package nordStorm.ProductsByRatingsOrderd.TotalOrderPartitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProductReviewCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	protected void map(LongWritable lineoffset, Text line, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) throws java.io.IOException ,InterruptedException {
	
		String[] words= line.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		String iString=words[17];
		Integer rating=new Integer(0);
		try 
		{
		    rating = Integer.parseInt(iString);
		    System.out.println("Rating is a number: " + rating);
		    //context.getCounter(RatingsTracker.Integer_String).increment(1);
		} 
		catch (NumberFormatException e) 
		{
		    if(iString.equals(""))
		    {
		        System.out.println("Rating is an empty string.");
		        //context.getCounter(RatingsTracker.Empty_String).increment(1);
		        
		    }
		    else if(iString.length() == 1)
		    {
		        System.out.println("Rating is a single char");
		        //context.getCounter(RatingsTracker.Single_Character).increment(1);
		    }
		    else
		    {
		        System.out.println("Rating is an non-intereger number");
		        //context.getCounter(RatingsTracker.Non_Integer_String).increment(1);
		    }
		    rating=0;
		    System.out.println("This caused " + e);
		}
		
		if(!(words[0].equalsIgnoreCase("id")))
				context.write(new Text(words[12]),new LongWritable(rating)) ;
		
	}
}
