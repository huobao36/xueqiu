package com.snowballfinance.kddc.job;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.lang.model.SourceVersion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
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

public class ItemSimilarityJob extends Configured implements Tool{

	private final static List<Long> itemIds = new LinkedList<Long>();
	private static Logger logger = Logger.getLogger(ItemSimilarityJob.class);
	
	public static void main(String[] args) 
	{
		if(args.length < 2)
			System.err.println("Usage: ItemSimilarityJob /path/to/input /path/to/output");
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new ItemSimilarityJob(), args);
		} catch (Exception e) {
			logger.error("JaccardIndexJob Tool Runner Err.", e);
		}
		System.exit(res);
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJobName("Item Similarity Job");
		job.setMapperClass(CateAndKeyWordMapper.class);
		job.setReducerClass(ScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.waitForCompletion(true);
		return 0;
	}
	
	private static final int FULLSCORE = 8 + 4 + 2 + 2; 
	
	private static float getCategorySimScore(String srcCat, String destCat) {
		String[] srcCats = srcCat.split("\\.");
		String[] destCats = destCat.split("\\.");
		float res = 0f;
		int minlen = Math.min(srcCats.length, destCats.length);
		if(minlen > 0)
		{
			if(srcCats[0].equals(destCats[0]))
				res += 8;
		}
		if(minlen > 1) {
			if(srcCats[1].equals(destCats[1]))
				res += 4;
		}
		if(minlen > 2) {
			if(srcCats[2].equals(destCats[2]))
				res += 2;
		}
		if(minlen > 3)
		{
			if(srcCats[3].equals(destCats[3]))
				res += 1;
		}
		return res;
	}
	
	
	private static float getKeyWordSimScore(String srcKeyWord, String destKeyWord) {
		String[] srcKeyWords = srcKeyWord.split(";");
		String[] destKeyWords = destKeyWord.split(";");
		Set<String> srcKeyWordSet = new HashSet<String>(Arrays.asList(srcKeyWords));
		Set<String> destKeyWordSet = new HashSet<String>(Arrays.asList(destKeyWords));
		int totalcount = srcKeyWordSet.size() + destKeyWordSet.size();
		srcKeyWordSet.retainAll(destKeyWordSet);
		int commoncount = srcKeyWordSet.size();
		return totalcount == 0 ? 0 : commoncount / (totalcount - commoncount);
	}
	

	private static class Item
	{
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getCategory() {
			return category;
		}
		public void setCategory(String cat) {
			this.category = cat;
		}
		public String getKeyWords() {
			return keywords;
		}
		public void setKeyWords(String keywords) {
			this.keywords = keywords;
		}
		String key;
		String category;
		String keywords;
	};
	
	private static List<Item> itemKeyValPairList = new LinkedList<Item>();
	
	public static class CateAndKeyWordMapper extends Mapper<LongWritable, Text, Text, FloatWritable>
	{
		private static float computeSocre(String srccat, String srckeywords, Item destItem )
		{
			float catScore = 0f;
			try {
				catScore = getCategorySimScore(srccat, destItem.getCategory());
			} catch (java.lang.ArrayIndexOutOfBoundsException e) {
				logger.error("CateAndKeyWordMapper getCategorySimScore Error. SRC:" + srccat + ", DEST:" + destItem.getCategory(), e);
			}
			return (getCategorySimScore(srccat, destItem.getCategory()) + getKeyWordSimScore(srckeywords, destItem.getKeyWords())) / 16;
		}
		
		@Override 
		protected void map(LongWritable ikey, Text ival,
				Context context)
				throws IOException, InterruptedException {
			String[] keyValues = ival.toString().split("\t");
			String key = keyValues[0];
			String cat = keyValues[1];
			String keywords = keyValues[2];
			for(Item item : itemKeyValPairList)
			{
				float score = computeSocre(cat, keywords, item);
				context.write(new Text(key + " " + item.getKey()), new FloatWritable(score));
			}
			Item curItem = new Item();
			curItem.setKey(key);
			curItem.setCategory(cat);
			curItem.setKeyWords(keywords);
			itemKeyValPairList.add(curItem);
		}
	}
	
	public static class ScoreReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
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
