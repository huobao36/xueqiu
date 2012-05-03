package com.snowballfinance.kddc.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class UserProfileJobTest {
	@Test
	public void testJob()
	{
		try {
			String[] args = {"/data/tmp/kddc/input/user_tiny_profile.txt", "/data/tmp/kddc/profileoutput"};
			ToolRunner.run(new Configuration(), new ItemSimilarityJob(), args);
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	
}
