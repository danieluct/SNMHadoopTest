package com.durda.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import com.durda.writables.UserCatKey;
import com.durda.writables.UserCatValue;

public class UserCatPartitioner extends Partitioner<UserCatKey, UserCatValue> {

	@Override
	public int getPartition(UserCatKey key, UserCatValue value,
			int numPartitions) {
		return key.getUserId().hashCode() % numPartitions;
	}

}
