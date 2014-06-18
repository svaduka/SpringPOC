package com.otsi.mapper.calc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.otsi.constants.IDFConstants;

public class CnumCalcMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	Map<String, Integer[]> emitingCols;
	private String fileName;
	@SuppressWarnings("rawtypes")
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		Integer[] colIdx=null;
		          
		emitingCols=new HashMap<String, Integer[]>();
		colIdx=new Integer[6];
		colIdx[0]=237;
		colIdx[1]=128;
		colIdx[2]=130;
		colIdx[3]=131;
		colIdx[4]=210;
		colIdx[5]=211;
		
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_QCI_FILENAME.toUpperCase(), colIdx);
		//Add Columns
		colIdx=null;
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_ESTABCAUSE_QCI_FILENAME.toUpperCase(), colIdx);
		colIdx=null;
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_DL_LAYER_CODEWORD_FILENAME.toUpperCase(), colIdx);
		
		colIdx=new Integer[6];
		colIdx[0]=293;
		colIdx[1]=294;
		colIdx[2]=298;
		colIdx[3]=299;
		colIdx[4]=303;
		colIdx[5]=304;
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_FILENAME, colIdx);
		colIdx=null;
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_ESTABCAUSE_FILENAME.toUpperCase(), colIdx);
		colIdx=new Integer[3];
		colIdx[0]=72;
		colIdx[1]=68;
		colIdx[2]=70;
		
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_TARGETEARFCNDL_FILENAME, colIdx);
		
		colIdx=new Integer[3];
		colIdx[0]=8;
		colIdx[1]=56;
		colIdx[2]=10;
		emitingCols.put(IDFConstants.IMediationFileNames.CNUM_TCID_HOCAUSE_FILENAME.toUpperCase(), colIdx);
		
		
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		String inputFileName=fileSplit.getPath().getName();
		fileName=getFileName(inputFileName).toUpperCase();
		
		}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		Integer[] cols=emitingCols.get(fileName);
		if(cols!=null)
		{
		String line=value.toString();
		String[] lineTokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(line, IDFConstants.DATA_FILE_DELIMITER);
		String mediationKey=lineTokens[2]+IDFConstants.VALUE_SEPERATOR+lineTokens[3]+IDFConstants.VALUE_SEPERATOR+lineTokens[5];
		StringBuffer sb=new StringBuffer();
		if(null!=lineTokens)
		{
		for (Integer colIdx : cols) {
			sb.append(lineTokens[colIdx]==null?"0":lineTokens[colIdx]+IDFConstants.VALUE_SEPERATOR);
		}
		}

		context.write(new Text(mediationKey), new Text(fileName+IDFConstants.FILENMAE_OUPUT_VALUE_SEPARATOR+sb.substring(0, sb.lastIndexOf(IDFConstants.VALUE_SEPERATOR)).toString()));
		}
	}
	
	
	private String getFileName(String inputFileName)
	{
		String fileName=null;
		if(null!=inputFileName ){
			if(inputFileName.indexOf("/")!=-1){
				inputFileName=inputFileName.substring(inputFileName.lastIndexOf("/")+1);
			}
			String[] tokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(inputFileName, IDFConstants.FILENAME_FIELDS_SEPERATOR);
			fileName=tokens[1];
		}
		return fileName;
	}
	
 }
