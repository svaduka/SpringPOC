package com.otsi.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Sainagaraju Vaduka
 * Date: 05/16/2014
 * This job is used to create KiteDatasets on HDFS.
 * Assumptions: The Data is available on HDFS
 *
 */
public class CreateKiteDatasetJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=super.getConf();
		
		Job createDatasetJob=new Job(conf, "CreateKiteDatasetJob");
		createDatasetJob.setJarByClass(CreateKiteDatasetJob.class);
		final String rawStorageDir=args[0];
		final String outDir=args[1];
		final String repoURL=args[2];
		
		
		
		return 0;
	}

	public static void main(String[] args) {
		if(args==null || args.length<3)
		{
			System.out.println("Usage: CreateKiteDatasetJob RawStorageDir OutputDir repoURL");
			System.out.println("Exiting the program");
		}
		
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			ToolRunner.run(conf, new CreateKiteDatasetJob(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
