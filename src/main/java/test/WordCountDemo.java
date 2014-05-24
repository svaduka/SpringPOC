package test;

import java.net.URI;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

public class WordCountDemo extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Configuration conf=new Configuration(Boolean.TRUE);
		try {
			ToolRunner.run(conf, new WordCountDemo(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=super.getConf();
		Job job=new Job(conf, "WordCount");
		job.setJarByClass(WordCountDemo.class);
		
		job.setMapperClass(WordCountMapper.class);
//		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		DatasetKeyOutputFormat.setRepositoryUri(job, new URI("repo:hdfs:/user/cloudera/KITE13/OP/TEST"));
		System.out.println(new URI("repo:hdfs:/user/cloudera/KITE13/OP/TEST"));
		DatasetKeyOutputFormat.setDatasetName(job, "Test1111");
		job.setOutputFormatClass(DatasetKeyOutputFormat.class);
		job.setMapOutputKeyClass(GenericRecord.class);
		job.setMapOutputValueClass(Void.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(Boolean.TRUE);
		return 0;
	}

}
