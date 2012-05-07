package com.snowballfinance.kddc.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
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
        
        JobConf job = new JobConf(conf, UserActionSnsJoinJob.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        
        job.setJobName("DataJoin");
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);
        
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TaggedWritable.class);
        JobClient.runJob(job); 
        return 0;
	}
	

	public static class JoinMapper extends DataJoinMapperBase {
		
		private final static String USER_ACTION_INPUT = "user_action.txt";
		private final static String USER_SNS_INPUT = "user_sns.txt";
		private final static String USER_ACTION_TAG = "0";
		private final static String USER_SNS_TAG = "1";
		
		
		@Override
		protected Text generateGroupKey(TaggedMapOutput aRecord) {
			String line = aRecord.getData().toString();
			String[] values = line.split("\t");
			System.out.println("generateGroupKey:" + values[0] + "," + values[1]);
			return new Text(values[0] + "," + values[1]);
		}

		@Override
		protected Text generateInputTag(String input) {
			String[] subFN = input.split("/");
			System.out.println("generateInputTage: input," + input);
			if(subFN != null)
			{
				input = subFN[subFN.length - 1];
			}
			if(input.equals(USER_ACTION_INPUT))
				return new Text(USER_ACTION_TAG);
			else if(input.equals(USER_SNS_INPUT))
				return new Text(USER_SNS_TAG);
			else 
				return null;
		}

		@Override
		protected TaggedMapOutput generateTaggedMapOutput(Object value) {
			TaggedWritable retv = null;
			
			if(this.inputTag != null && this.inputTag.toString().equals(USER_SNS_TAG))
			{
				StringBuffer sb = new StringBuffer();
				sb.append(value.toString()).append('\t').append("1");
				retv = new TaggedWritable(new Text(sb.toString()));		
			}
			else
				retv = new TaggedWritable((Text) value);
            retv.setTag(this.inputTag);
			System.out.println("generateTaggedMapOutput: value," + retv.getData().toString());
			System.out.println("generateTaggedMapOutput: tag," + retv.getTag().toString());

            return retv;
		}
	}
	
    public static class JoinReduce extends DataJoinReducerBase {
        
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
        	for(Object obj : tags) {
        		Text tag = (Text) obj;
        		System.out.println("TaggedMapOutput: tag" + tag.toString() );
        	}
        	
            String joinedStr = ""; 
            for (int i=0; i < values.length; i++) {
                if (i > 0) 
                	joinedStr += "\t";
                TaggedWritable tw = (TaggedWritable) values[i];
                String line = ((Text) tw.getData()).toString();
                
                String[] tokens = line.split("\t");
                int len = tokens.length;
                if(tokens != null)
                {
                	for(int j = 2; j < len; ++j)
                	{
                		joinedStr += tokens[j];
                	}
                }
                
                if(tokens != null)
                	joinedStr += tokens[tokens.length - 1];
            }
            TaggedWritable retv = new TaggedWritable(new Text(joinedStr));
            retv.setTag((Text) tags[0]); 
            return retv;
        }
    }
    
    public static class TaggedWritable extends TaggedMapOutput {
        
        private Text data;
        
        public TaggedWritable() {
        	
        }
        
        public TaggedWritable(Text data) {
            this.tag = new Text("");
            this.data = data;
        }
        
        public Writable getData() {
            return data;
        }
        
        public void write(DataOutput out) throws IOException {
            out.writeBytes(tag + "\n");
        	out.write(data.getBytes());
        }
        
        public void readFields(DataInput in) throws IOException {
        	tag = new Text(in.readLine());
        	System.out.println("readFields:" + tag.toString());
        	data = new Text(in.readLine());
        	System.out.println("readFields:" + data.toString());
        }
    }

}
