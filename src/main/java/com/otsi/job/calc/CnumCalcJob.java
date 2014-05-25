package com.otsi.job.calc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CnumCalcJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
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
