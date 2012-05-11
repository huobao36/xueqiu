package com.snowballfinance.kddc.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

public class MahoutRecommenderJob {
	private static Logger logger = Logger.getLogger(MahoutRecommenderJob.class); 
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Configuration(), new RecommenderJob(), args);
		} catch (Exception e) {
			logger.error("MahoutRecommenderJob Error." , e);
		}
	}
	
	
}
