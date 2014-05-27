package com.otsi.mapper.calc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.otsi.constants.IDFConstants;

public class CnumCalcMapper extends Mapper<LongWritable, Text, Text, Text> {

	String fileName;
	String expressionValuesInJson;
	Set<Integer> columnsToEmit=null;
	
	@SuppressWarnings("rawtypes")
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		String inputFileName=fileSplit.getPath().getName();
		fileName=getFileName(inputFileName);
		String expressionValues=conf.get(IDFConstants.IConfParameters.PROP_EXPRESSION_COL_JSON);
		try {
			JSONObject json=new JSONObject(expressionValues);
			Object obj=json.get(fileName);
			columnsToEmit=new HashSet<Integer>();
			if(obj instanceof JSONArray)
			{
				JSONArray array=(JSONArray)obj;
				for(int i=0;i<array.length();i++)
				{
					columnsToEmit.add(array.getInt(i));
				}
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String inputLine=value.toString();
		String[] lineTokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(inputLine, IDFConstants.DATA_FILE_DELIMITER);
		String levelKey=lineTokens[2]+"_"+lineTokens[3]+"_"+lineTokens[4];
		StringBuffer buffer=new StringBuffer();
		for (Integer colIdx : columnsToEmit) {
			buffer.append(lineTokens[colIdx]==null?colIdx+":"+"0":colIdx+":"+lineTokens[colIdx]+"_");
		}
		String emitingValues=buffer.substring(0, buffer.lastIndexOf("_")).toString();
		context.write(new Text(levelKey), new Text(fileName+"\t"+emitingValues));

	}
	
	private String getFileName(String inputFileName)
	{
		String fileName=null;
		if(null!=inputFileName ){
			if(inputFileName.indexOf("/")!=-1){
				inputFileName=inputFileName.substring(inputFileName.lastIndexOf("/")+1);
			}
			String[] tokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(inputFileName, IDFConstants.FILENAME_FIELDS_SEPERATOR, 2);
			fileName=tokens[0];
		}
		return fileName;
	}
}
