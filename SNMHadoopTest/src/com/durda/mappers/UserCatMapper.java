package com.durda.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.durda.writables.UserCatKey;
import com.durda.writables.UserCatValue;

public class UserCatMapper extends
		Mapper<LongWritable, Text, UserCatKey, UserCatValue> {

	private UserCatKey psK = new UserCatKey();
	private UserCatValue psV = new UserCatValue();

  //just read from temp file and pass along to reducer
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] elems = value.toString().split("þ");

		// something's wrong, ignore line
		if (elems.length != 7)
			return;
		System.out.println(value);
		psK.set(Integer.parseInt(elems[0]), elems[1]);
		psV.setTotal(Integer.parseInt(elems[2]));
		for (int i = 3; i < 7; i++) {
			psV.setQrt(i - 3, Double.parseDouble(elems[i]));
		}

		context.write(psK, psV);

	}

}
