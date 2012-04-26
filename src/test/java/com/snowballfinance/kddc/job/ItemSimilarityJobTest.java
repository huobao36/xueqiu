package com.snowballfinance.kddc.job;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;

public class ItemSimilarityJobTest {
	
	private void deleteFile(File file){ 
		if(file.exists()){ 
			if(file.isFile()){ 
				file.delete(); 
			} else if(file.isDirectory()){ 
				File files[] = file.listFiles(); 
				for(int i=0;i<files.length;i++){ 
					this.deleteFile(files[i]); 
				}
		     } 
		    file.delete(); 
		}else{ 
		    System.out.println("所删除的文件不存在！"+'\n'); 
		}
	} 
	
	@Test
	public void testJob()
	{
		String args[] = {"/Users/louis36/experiment/kddc_data/tmp/item-simlarity-input", "/Users/louis36/experiment/kddc_data/tmp/item-similarity-output"};
		try {
			ToolRunner.run(new Configuration(), new ItemSimilarityJob(), args);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
