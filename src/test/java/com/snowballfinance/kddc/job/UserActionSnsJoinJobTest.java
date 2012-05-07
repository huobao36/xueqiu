package com.snowballfinance.kddc.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class UserActionSnsJoinJobTest {
	@Test
	public void jobTest() {
		try {
			String[] args = {"/data/tmp/kddc/input", "/data/tmp/kddc/action-sns-join-output"};
			ToolRunner.run(new Configuration(), new UserActionSnsJoinJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
