package com.durda.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.durda.writables.UserCatKey;
import com.durda.writables.UserCatValue;

public class UserCatReducer extends
		Reducer<UserCatKey, UserCatValue, IntWritable, Text> {

	int lastUserId = -1;
	List<String> bestCats = new ArrayList<String>();
	int bestQuant = -1;
	List<double[]> bestQrts = new ArrayList<double[]>();

	public static String formatOutput(String category, double[] qrts) {
		StringBuilder sb = new StringBuilder();
		sb.append(category).append("\t");
		for (int i = 0; i < 4; i++) {
			if (qrts[i] == 0)
				sb.append("-");
			else
				sb.append(qrts[i]);
			if (i < 3)
				sb.append("\t");
		}

		return sb.toString();
	}

	IntWritable userKey = new IntWritable();

	@Override
	public void reduce(UserCatKey key, Iterable<UserCatValue> values,
			Context context) throws IOException, InterruptedException {

		int userId = key.getUserId().get();

		List<String> bestCats = new ArrayList<String>();
		int bestQuant = -1;
		List<double[]> bestQrts = new ArrayList<double[]>();

		String lastCat = "";
		int total = 0;
		double[] qrts = { 0.0, 0.0, 0.0, 0.0 };

		for (UserCatValue value : values) {
			String curCat = key.getCategory();
			if (lastCat != curCat) {
				if (total > 0) {
					if (total > bestQuant) {
						bestCats.clear();
						bestCats.add(lastCat);
						bestQuant = total;
						bestQrts.clear();
						bestQrts.add(qrts.clone());
					} else if (total == bestQuant) {
						bestCats.add(key.getCategory().toString());
						bestQrts.add(qrts.clone());
					}
				}
				lastCat = curCat;
				total = 0;
				for (int i = 0; i < 4; i++) {
					qrts[i] = 0.0;
				}
			}

			total += value.getTotal();
			for (int i = 0; i < 4; i++)
				qrts[i] += value.getQrt(i);
		}

		if (total > 0) {
			if (total > bestQuant) {
				bestCats.clear();
				bestCats.add(lastCat);
				bestQuant = total;
				bestQrts.clear();
				bestQrts.add(qrts.clone());
			} else if (total == bestQuant) {
				bestCats.add(key.getCategory().toString());
				bestQrts.add(qrts.clone());
			}
		}

		userKey.set(userId);
		for (int i = 0; i < bestCats.size(); i++) {
			String bestCat = bestCats.get(i);
			Text out = new Text();
			out.set(formatOutput(bestCat, bestQrts.get(i)));
			context.write(userKey, out);
		}
	}

}
