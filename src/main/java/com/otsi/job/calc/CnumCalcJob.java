package com.otsi.job.calc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.otsi.mapper.calc.CnumCalcMapper2;
import com.otsi.reducer.calc.CnumCalCombiner;
import com.otsi.reducer.calc.CnumCalcReducer2;

public class CnumCalcJob extends Configured implements Tool {

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		long iStartMillis=System.currentTimeMillis();
		
		Configuration conf= super.getConf();
		Job job=new Job(conf, "CnumCalcJob");
		job.setJarByClass(CnumCalcJob.class);
		
		job.setMapperClass(CnumCalcMapper2.class);
//		job.setCombinerClass(CnumCalCombiner.class);
		job.setReducerClass(CnumCalcReducer2.class);
//		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.waitForCompletion(Boolean.TRUE);
		
		System.out.println("Job Completed in secs(in millis)"+(System.currentTimeMillis()-iStartMillis));
		return 0;
	}
	
	public static void main(String[] args) {
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			
			int i=ToolRunner.run(conf, new CnumCalcJob(), args);
			System.exit(i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
