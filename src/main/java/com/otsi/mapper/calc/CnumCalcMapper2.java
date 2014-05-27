package com.otsi.mapper.calc;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CnumCalcMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	Map<String, Integer[]> emitingCols;
	@SuppressWarnings("rawtypes")
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		Integer[] colIdx=new Integer[6];
				  colIdx[0]=1;
		          colIdx[1]=3;
		          
		emitingCols.put("CNUM", colIdx);
		emitingCols.put("CNUM", colIdx);
		emitingCols.put("CNUM", colIdx);
		emitingCols.put("CNUM", colIdx);
		}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

	}
	
 }
