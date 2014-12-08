package com.durda.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.durda.writables.UserCatKey;

public class UserCatComparator extends WritableComparator {

	public UserCatComparator() {
		super(UserCatKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UserCatKey userKey1 = (UserCatKey) a;
		UserCatKey userKey2 = (UserCatKey) b;
		return userKey1.getUserId().compareTo(userKey2.getUserId());
	}

}
