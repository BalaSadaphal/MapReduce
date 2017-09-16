package nordStorm.productRatings.SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BrandGroupComparator extends WritableComparator {
	protected BrandGroupComparator() {
		super(TextIntWritablePair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TextIntWritablePair key1 = (TextIntWritablePair) w1;
		TextIntWritablePair key2 = (TextIntWritablePair) w2;
		return key1.getBrand().compareTo(key2.getBrand());
	}

}
