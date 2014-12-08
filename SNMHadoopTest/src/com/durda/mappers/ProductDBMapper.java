package com.durda.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.durda.writables.ProductDBWritable;
import com.durda.writables.ProductSmartKey;
import com.durda.writables.ProductSmartValue;

public class ProductDBMapper
		extends
		Mapper<LongWritable, ProductDBWritable, ProductSmartKey, ProductSmartValue> {

	ProductSmartKey psK = new ProductSmartKey();
	ProductSmartValue psV = new ProductSmartValue();

	@Override
	protected void map(LongWritable key, ProductDBWritable value,
			Context context) throws IOException, InterruptedException {

		// there's no User with ID = -1 in the database, isn't it?
		psK.set(value.getId(), -1);
		psV.set(value.getCategory(), value.getPrice());
		context.write(psK, psV);

	}

}
