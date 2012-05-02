package com.snowballfinance.kddc.job;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.common.AbstractJob;


public class UserProfileJob extends AbstractJob{
	
	private static Logger logger = Logger.getLogger(UserProfileJob.class);
	
	@Override
	public int run(String[] arg0) throws Exception {
		addInputOption();
		addOutputOption();
		Job simJob = prepareJob(getInputPath(), getOutputPath(), UserProfileMapper.class, 
				LongWritable.class, Text.class, UserProfileSimReducer.class, Text.class, FloatWritable.class);
		simJob.setCombinerClass(UserProfileSimReducer.class);
		simJob.waitForCompletion(true);
		return 0;
	}
	
	private static class UserProfile
	{
		public long getUid() {
			return uid;
		}
		public void setUid(long uid) {
			this.uid = uid;
		}
		public long getBirthYear() {
			return birthYear;
		}
		public void setBirthYear(long birthYear) {
			this.birthYear = birthYear;
		}
		public int getSexuality() {
			return sexuality;
		}
		public void setSexuality(int sexuality) {
			this.sexuality = sexuality;
		}
		public String getTags() {
			return tags;
		}
		public void setTags(String tags) {
			this.tags = tags;
		}
		public int getTweetCount() {
			return tweetCount;
		}
		public void setTweetCount(int count) {
			this.tweetCount = count;
		}
		private	long uid;
		private long birthYear;
		private int  sexuality;
		private int  tweetCount;
		private String tags;
	}
	private static List<UserProfile> userProfileList = new LinkedList<UserProfile>();
	
	public static class UserProfileMapper extends Mapper<LongWritable, Text, Text, FloatWritable>
	{
		private static float computeSocre(String srctags, String desttags)
		{
			float score = 0f;
			try {
				String[] srcTags = srctags.split(";");
				String[] destTags = desttags.split(";");
				Set<String> srcKeyWordSet = new HashSet<String>(Arrays.asList(srcTags));
				Set<String> destKeyWordSet = new HashSet<String>(Arrays.asList(destTags));
				int totalcount = srcKeyWordSet.size() + destKeyWordSet.size();
				srcKeyWordSet.retainAll(destKeyWordSet);
				int commoncount = srcKeyWordSet.size();
				return totalcount == 0 ? 0 : commoncount / (totalcount - commoncount);
			} catch (java.lang.ArrayIndexOutOfBoundsException e) {
				logger.error("UserProfileMapper computeSocre Error. SRC:" + srctags + ", DEST:" + desttags, e);
			}
			return score;
		}
		
		@Override 
		protected void map(LongWritable ikey, Text ival,
				Context context)
				throws IOException, InterruptedException {
			String[] keyValues = ival.toString().split("\t");
			String uid = keyValues[0];
			String year = keyValues[1];
			String sexuality = keyValues[2];
			String tweetCount = keyValues[3];
			String tags = keyValues[4];
			for(UserProfile profile : userProfileList)
			{
				float score = computeSocre(tags, profile.getTags());
				context.write(new Text(uid + " " + profile.getUid()), new FloatWritable(score));
			}
			UserProfile curProfile = new UserProfile();
			curProfile.setUid(Long.valueOf(uid));
			curProfile.setTags(tags);
			userProfileList.add(curProfile);
		}
	}
	
	public static class UserProfileSimReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
	{
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> scores, Context ctx)
			throws IOException, InterruptedException 
		{
			Iterator<FloatWritable> iter = scores.iterator();
			float score = iter.next().get();
			ctx.write(key, new FloatWritable(score));
			String[] keyPairs = key.toString().split(" ");
			ctx.write(new Text(keyPairs[1] + " " + keyPairs[0]), new FloatWritable(score));
		}
	}
  
}
