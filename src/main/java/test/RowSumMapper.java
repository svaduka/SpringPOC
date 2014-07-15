package test;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RowSumMapper extends Mapper<LongWritable ,Text , IntWritable,IntWritable> {
	public void map(LongWritable key, Text value,Context context)
	{
		String line=value.toString();
		String [] str=StringUtils.splitByWholeSeparator(line, ",");
		int n=str.length;
		int number;
		for(int i=0;i<n;i++)
		{
			 number = Integer.parseInt(str[i]);
			 
		
	
		try {
			context.write(new IntWritable(number), new IntWritable(1));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
	

}
}
