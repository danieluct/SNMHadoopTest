package com.durda.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import com.durda.writables.ProductSmartKey;
import com.durda.writables.ProductSmartValue;

public class ProductSmartPartitioner extends
		Partitioner<ProductSmartKey, ProductSmartValue> {

	@Override
	public int getPartition(ProductSmartKey key, ProductSmartValue value,
			int numPartitions) {
		return key.getId().hashCode() % numPartitions;
	}

}
