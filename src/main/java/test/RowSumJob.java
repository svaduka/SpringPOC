package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class RowSumJob extends Configured implements Tool {
	
	public static void main(String [] args)
	{
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			ToolRunner.run(conf, new RowSumJob(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		
		// TODO Auto-generated method stub
		return 0;
	}
	

}
