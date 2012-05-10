package com.snowballfinance.kddc.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class FilterJobTest {

	@Test
	public void testJob() 
	{
		String[] args = {"/data/tmp/kddc/user-item-input", "/data/tmp/kddc/user-item-output"};
		try {
			ToolRunner.run(new Configuration(), new FilterItemJob(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
