package com.snowballfinance.kddc.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



// The DataJoinMapper user the old api 

public class UserActionSnsJoinJob extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(UserActionSnsJoinJob.class);
	
	public static void main(String[] args)
	{
		if(args.length < 2)
			System.err.println("Usage: UserActionSnsJoinJob /path/to/input /path/to/output");
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new UserActionSnsJoinJob(), args);
		} catch (Exception e) {
			logger.error("UserActionSnsJoinJob Tool Runner Err.", e);
		}
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(UserActionSnsJoinJob.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserActionSnsWritable.class);
		job.setCombinerClass(JoinReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
        return 0;
	}
	
	private static class UserActionSnsWritable implements Writable
	{
		public boolean isFollowed() {
			return followed;
		}
		public void setFollowed(boolean followed) {
			this.followed = followed;
		}
		public String getActions() {
			return actions;
		}
		public void setActions(String actions) {
			this.actions = actions;
		}
		private boolean followed;
		private String actions;
		
		@Override
		public void readFields(DataInput in) throws IOException {
			actions = in.readLine();
			followed = in.readBoolean();
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeChars(actions + "\n");
			out.writeBoolean(followed);
		} 
		
	}
	
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, UserActionSnsWritable>
	{
		@Override 
		protected void map(LongWritable ikey, Text ival,
				Context context)
				throws IOException, InterruptedException {
			String[] keyValues = ival.toString().split("\t");
			if(keyValues != null)
			{
				int len = keyValues.length;
				if(len == 5) {
					String key = keyValues[0] + "\t" + keyValues[1];
					String actions = keyValues[2] + "\t" + keyValues[3] + "\t" + keyValues[4];
					UserActionSnsWritable value = new UserActionSnsWritable();
					value.setActions(actions);
					context.write(new Text(key), value);
				}else if(len == 2) {
					String key = keyValues[0] + "\t" + keyValues[1];
					UserActionSnsWritable value = new UserActionSnsWritable();
					value.setFollowed(true);
					context.write(new Text(key), value);
				}
			}
		}
	}
	
	public static class JoinReducer extends Reducer<Text, UserActionSnsWritable, Text, IntWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<UserActionSnsWritable> actions, Context ctx)
			throws IOException, InterruptedException 
		{
			int atCount = 0;
			int retweetCount = 0;
			int commentCount = 0;
			boolean followed = false;
			
			Iterator<UserActionSnsWritable> iter = actions.iterator();
			while(iter.hasNext())
			{
				UserActionSnsWritable actionItem = iter.next();
				String[] actionStrs = actionItem.getActions().split("\t");
				if(actionStrs != null && actionStrs.length >= 3)
				{
					String str0 = actionStrs[0].trim();
					String str1 = actionStrs[1].trim();
					String str2 = actionStrs[2].trim();
					try {
						atCount += Integer.valueOf(str0, 10);
						retweetCount += Integer.valueOf(str1, 10);
						commentCount += Integer.valueOf(str2, 10);
					} catch (java.lang.NumberFormatException e) {
						logger.error("Join Reduce Parse Err:" + str0 + "," + str1 +"," + str2, e);
					}
				} else if(actionItem.isFollowed())
					followed = true; 
			}
			int score = atCount * 4 + retweetCount * 2 + commentCount + (followed ? 8 : 0); 
			String[] src_dest = key.toString().split("\t");
			if(src_dest[0].equals(src_dest[1]))
				ctx.write(key, new IntWritable(0));
			else
				ctx.write(key, new IntWritable(score));
		}
	}
}
