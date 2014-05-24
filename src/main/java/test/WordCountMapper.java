package test;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String schemaStr="{ \"type\": \"record\", \"name\": \"User\", \"namespace\": \"org.kitesdk.examples.data\", \"doc\": \"A user record\", \"fields\": [ { \"name\": \"username\", \"type\": \"string\" , \"default\": \"NONE\"}, { \"name\": \"creationDate\", \"type\": \"long\" , \"default\": 0 }, { \"name\": \"favoriteColor\", \"type\": \"string\", \"default\": \"NONE\" } ] }";
		Schema schema=new Schema.Parser().parse(schemaStr);
		
		context.write(new GenericRecordBuilder(schema).build(), null);
	}
}
