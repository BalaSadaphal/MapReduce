package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BrandReviewSortComparator extends WritableComparator{
	
	protected BrandReviewSortComparator() {
	
		super(TextIntWritablePair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextIntWritablePair key1 = (TextIntWritablePair) w1;
		TextIntWritablePair key2 = (TextIntWritablePair) w2;

		int cmpResult = key1.getBrand().compareTo(key2.getBrand());
		if (cmpResult == 0)// same Brand
		{
			return -key1.getReview()
					.compareTo(key2.getReview());
			//If the minus is taken out, the values will be in
			//ascending order
		}
		return cmpResult;
	}

}
