package secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntWritablePair implements WritableComparable{
	
	private Text brand;
	private IntWritable review;
	
	public TextIntWritablePair(Text brand, IntWritable review)
	{
		set(brand,review);
	}
	
	public void set(Text brand, IntWritable review){
	this.brand=brand;
	this.review=review;
	}
	
	public Text getBrand() {
	return brand;
	}
	
	public IntWritable getReview() {
	return review;
	}
	
	public TextIntWritablePair(){
	set(new Text(),new IntWritable());
	}
	
	public TextIntWritablePair(String brand, Integer review){
	set(new Text(brand),new IntWritable(review));
	}
	
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		brand.readFields(in);
		review.readFields(in);
		
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		brand.write(out);
		review.write(out);
		
	}

	
	public int compareTo(Object o) {
		TextIntWritablePair tp = (TextIntWritablePair) o;
		int cmp = brand.compareTo(tp.brand);
		
		if(cmp==0){
			return -(review.compareTo(tp.review));
		}
		return cmp;
		
	}
/*
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		System.out.println("I am in Object Compare TO");
		return 0;
	}
	*/
	@Override
	public String toString() {
		// TODO Auto-generated method stub
	
		return brand.toString() + " | "+ review.toString();
	}
	
	public int hashCode(){
		return brand.hashCode()*163 ;
	}
	public boolean equals(Object o){
		if( o instanceof TextIntWritablePair){
		TextIntWritablePair tp = (TextIntWritablePair) o;
		return brand.equals(tp.brand) && review.equals(tp.review);
		}
		return false;
	}
	
	

	

}
