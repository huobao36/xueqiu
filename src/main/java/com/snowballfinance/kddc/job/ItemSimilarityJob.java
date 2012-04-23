package com.snowballfinance.kddc.job;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import javax.lang.model.SourceVersion;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class ItemSimilarityJob extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		return 0;
	}

	private float getCategorySimScore(String srcCat, String destCat) {
		String[] srcCats = srcCat.split(".");
		String[] destCats = destCat.split(".");
		float res = 0f;
		if(srcCats[0].equals(destCats[0]))
			res += 8;
		if(srcCats[1].equals(destCats[1]))
			res += 4;
		if(srcCats[2].equals(destCats[2]))
			res += 2;
		if(srcCats[3].equals(destCats[3]))
			res += 1;
		return res;
	}
	
	
	private float getKeyWordSimScore(String srcKeyWord, String destKeyWord) {
		String[] srcKeyWords = srcKeyWord.split(";");
		String[] destKeyWords = destKeyWord.split(";");
		Set<String> srcKeyWordSet = new HashSet<String>(Arrays.asList(srcKeyWords));
		Set<String> destKeyWordSet = new HashSet<String>(Arrays.asList(destKeyWords));
		int totalcount = srcKeyWordSet.size() + destKeyWordSet.size();
		srcKeyWordSet.retainAll(destKeyWordSet);
		int commoncount = srcKeyWordSet.size();
		return totalcount == 0 ? 0 : commoncount / (totalcount - commoncount);
	}
	
	public static class CateAndKeyWordMapper extends Mapper<LongWritable, Text, Text, StrFloatPair>
	{
		
		@Override 
		protected void map(LongWritable ikey, Text ival,
				Context context)
				throws IOException, InterruptedException {
			String[] keyValues = ival.toString().split(" ");
			String key = keyValues[0];
			
		}
	}
	
	
}
