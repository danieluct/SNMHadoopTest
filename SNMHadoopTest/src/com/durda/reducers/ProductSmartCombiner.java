package com.durda.reducers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.durda.writables.ProductSmartKey;
import com.durda.writables.ProductSmartValue;

public class ProductSmartCombiner extends
		Reducer<ProductSmartKey, ProductSmartValue, ProductSmartKey, ProductSmartValue> {

	ProductSmartKey ucK=new ProductSmartKey();
	ProductSmartValue ucV=new ProductSmartValue();

	//agreggate quarterly data
	@Override
	public void reduce(ProductSmartKey key, Iterable<ProductSmartValue> values,
			Context context) throws IOException, InterruptedException {

			int[] qrts = { 0, 0, 0, 0 };
			String quarter;
			double quantity;

			for (ProductSmartValue value : values) {
				quarter = value.getFirstVal().toString();
				quantity = value.getSecndVal();

				if (quarter.equals("q1")) {
					qrts[0] += quantity;
				} else if (quarter.equals("q2")) {
					qrts[1] += quantity;
				} else if (quarter.equals("q3")) {
					qrts[2] += quantity;
				} else if (quarter.equals("q4"))
				{
					qrts[3] += quantity;
				}
        else
        {
        context.write(key, value);
        continue;
        }
			}
			ucK.set(key.getId().get(),key.getUserId());
			for(int i=0;i<4;i++)
			{
				if(qrts[i]>0)
				{
					ucV.set("q"+(i+1), qrts[i]);
					context.write(ucK, ucV);
				}				
			}
		}

}
