package secondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class BrandPartitioner extends Partitioner{
	
	public int getPartition(TextIntWritablePair key, Text value,
			int numReduceTasks) {
		String brand=key.getBrand().toString().trim();
		System.out.println(brand);
		
		if(brand.equals("Amazon"))
			return 0;
			else
				return 1;
		//return (key.getBrand().hashCode() % numReduceTasks);
	}

	@Override
	public int getPartition(Object o1, Object o2, int numReduceTasks) {
		// TODO Auto-generated method stub
		TextIntWritablePair key=(TextIntWritablePair)o1;
		String brand=key.getBrand().toString().trim();
		System.out.println(brand);
		
		if(brand.equals("Amazon"))
			return 0;
			else
				return 1;
	}

}
