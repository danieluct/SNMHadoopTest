package com.durda.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.durda.writables.ProductSmartKey;

public class ProductSmartComparator extends WritableComparator {

	public ProductSmartComparator() {
		super(ProductSmartKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ProductSmartKey productKey1 = (ProductSmartKey) a;
		ProductSmartKey productKey2 = (ProductSmartKey) b;
		return productKey1.getId().compareTo(productKey2.getId());
	}

}
