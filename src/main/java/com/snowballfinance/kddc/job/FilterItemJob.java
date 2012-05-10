package com.snowballfinance.kddc.job;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



public class FilterItemJob extends Configured implements Tool {

	private static Logger logger = Logger.getLogger(FilterItemJob.class);
	private static final String UID_FN = "uid.txt";
	private static final String ITEM_FN = "item.txt";
	
	public static void main(String[] args)
	{
		logger.info("FilterItemJob start.");
		if(args.length < 2)
		{
			System.err.println("Usage: FilterItemJob /path/to/input /path/to/output. \n" +
					"The input directory should contain two files: uid.txt, item.txt");
		}
		int res = 0;
		try {
			res = ToolRunner.run(new Configuration(), new FilterItemJob(), args);
		} catch (Exception e) {
			logger.error("FilterItemJob Tool Runner Err.", e);
		}
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
        Configuration conf = getConf();
		Job job = new Job(conf);
		job.setJarByClass(FilterItemJob.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BooleanWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
        return 0;
	}
	
	private static class FilterMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {
		@Override
		protected void setup(Context context
                  ) throws IOException, InterruptedException {
		    super.setup(context);  
		}

		private String getInputFN(Context context) 
		{
		    FileSplit fs = (FileSplit) context.getInputSplit();
		    logger.info("FilterMappder fs:" + fs.toString());
		    String pathName = fs.getPath().getName();
		    logger.info("FilterMappder path:" + pathName);
		    String[] paths = pathName.split("/");
		    if(paths != null && paths.length > 0)
		    	return paths[paths.length - 1];
		    else
		    	return "";
		}
		
		@Override 
		protected void map(LongWritable ikey, Text ival,
				Context context)
				throws IOException, InterruptedException {
			String fn = getInputFN(context);
			logger.info("FilterMapper File Name: " + fn);
			if(fn.equals(UID_FN))
			{
				context.write(ival, new BooleanWritable(true));
			} else if(fn.equals(ITEM_FN)) {
				context.write(ival, new BooleanWritable(false));
			}
		}
	}
	
	private static class FilterReduce extends Reducer<Text, BooleanWritable, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<BooleanWritable> ids, Context ctx)
			throws IOException, InterruptedException {
			Iterator<BooleanWritable> iter = ids.iterator();
			boolean write = true;
			while(iter.hasNext())
			{
				BooleanWritable bw = iter.next();
				write = (write && bw.get());
			}
			if(write)
				ctx.write(key, new Text(""));
			else
				logger.info("FilterReduce filter:" + key);
		}
	}
	
}
