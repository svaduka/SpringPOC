package com.otsi.job.calc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.otsi.mapper.calc.CnumCalcMapper;
import com.otsi.reducer.calc.CnumCalcReducer;

public class CnumCalcJob extends Configured implements Tool {

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf= super.getConf();
		Job job=new Job(conf, "CnumCalcJob");
		job.setJarByClass(CnumCalcJob.class);
		
		job.setMapperClass(CnumCalcMapper.class);
		job.setReducerClass(CnumCalcReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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
