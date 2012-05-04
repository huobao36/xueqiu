package com.snowballfinance.kddc.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
public class UserProfileJobTest   {
	@Test
	public void testJob()
	{
		try {
			String[] args = new String[]{"--input", "/data/tmp/kddc/user_tiny_profile.txt", "--output", "/data/tmp/kddc/profileoutput"};
			Configuration conf = new Configuration();
			UserProfileJob job = new UserProfileJob();
			job.setConf(conf);
			job.run(args);
		} catch (Exception e) {
			System.err.println(e);
		}
	}
	
}
