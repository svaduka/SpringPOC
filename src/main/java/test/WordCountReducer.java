package test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text key, Iterable<IntWritable> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, new IntWritable(1));
	}
}
