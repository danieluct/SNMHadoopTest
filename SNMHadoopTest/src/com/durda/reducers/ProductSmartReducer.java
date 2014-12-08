package com.durda.reducers;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.durda.writables.ProductSmartKey;
import com.durda.writables.ProductSmartValue;

public class ProductSmartReducer extends
		Reducer<ProductSmartKey, ProductSmartValue, Text, NullWritable> {

	private static Text converToText(int userId, String cat, int total,
			int[] qrt, double price) {
		Text txt = new Text();
		StringBuilder sb = new StringBuilder();
		sb.append(userId).append("þ").append(cat);
		sb.append("þ").append(total);
		for (int i = 0; i < 4; i++) {
			sb.append("þ").append(qrt[i] * price);
		}
		txt.set(sb.toString());
		return txt;

	}

	// due to custom Grouping Comparator, each reduce gets all results for one
	// productID
	@Override
	public void reduce(ProductSmartKey key, Iterable<ProductSmartValue> values,
			Context context) throws IOException, InterruptedException {

		String category = "";
		double price = -1;

		int lastUserId = -1;
		int[] qrts = { 0, 0, 0, 0 };
		double quantity;
		String quarter;

		for (ProductSmartValue value : values) {
			// first value always comes from the DB
			// because the keys are sorted and all users have userid>0
			if (price == -1) {
				price = value.getSecndVal();
				category = value.getFirstVal();
			} else {
				int curUser = key.getUserId();
				// users already came sorted, let's aggregate quantities
				if (lastUserId != curUser) {
					// user changed, time to unload data
					if (lastUserId != -1) {
						int total = qrts[0] + qrts[1] + qrts[2] + qrts[3];
						if (total > 0)
							context.write(
									converToText(lastUserId, category, total,
											qrts, price), NullWritable.get());
					}
					// reset aggregators
					for (int i = 0; i < 4; i++)
						qrts[i] = 0;
					lastUserId = curUser;
				}

				quarter = value.getFirstVal().toString();
				quantity = value.getSecndVal();

				if (quarter.equals("q1")) {
					qrts[0] += quantity;
				} else if (quarter.equals("q2")) {
					qrts[1] += quantity;
				} else if (quarter.equals("q3")) {
					qrts[2] += quantity;
				} else
					qrts[3] += quantity;

			}
		}

		int total = qrts[0] + qrts[1] + qrts[2] + qrts[3];
		if (total > 0)
			context.write(
					converToText(lastUserId, category, total, qrts, price),
					NullWritable.get());
	}

}
