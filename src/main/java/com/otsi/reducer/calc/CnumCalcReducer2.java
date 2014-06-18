package com.otsi.reducer.calc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.otsi.constants.IDFConstants;

public class CnumCalcReducer2 extends Reducer<Text, Text, Text, Text> {
	
	
	
	
	Map<String, Integer[]> emitingCols;

	
	@SuppressWarnings("rawtypes")
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
		}

	protected void reduce(Text key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		Integer.parseInt("Executing "+values);

		if (values != null) {
			double[] cNumAggValues = null;
			double[] cNum_QCIAggValues = null;
			double[] cNum_EstabCause_QCIAggValues = null;
			double[] cNum_DL_Layer_codewordAggValues = null;
			double[] cNum_EstabCauseAggValues = null;
			double[] cNum_TargetEarfcnDlAggValues = null;
			double[] cNum_tcID_HoCauseAggValues = null;

			//CNUM 0_37.5_23_66_25
			//ESTAB 0_34_9_0
			for (Text inputText : values) {
				String inputString=inputText.toString();
				//TOken Name value
				String rowTokens[] = StringUtils.splitByWholeSeparatorPreserveAllTokens(inputString, IDFConstants.FILENMAE_OUPUT_VALUE_SEPARATOR);
				String fileName=rowTokens[0].toUpperCase();//CNUM
				String valueRow=rowTokens[1];//0_37.5_23_66_25
				if(IDFConstants.IMediationFileNames.CNUM_DL_LAYER_CODEWORD_FILENAME.equalsIgnoreCase(fileName))
				{
					cNum_DL_Layer_codewordAggValues=populateValues(valueRow, cNum_DL_Layer_codewordAggValues);
				}else if(IDFConstants.IMediationFileNames.CNUM_ESTABCAUSE_FILENAME.equalsIgnoreCase(fileName))
				{
					cNum_EstabCauseAggValues=populateValues(valueRow, cNum_EstabCauseAggValues);
				}else if(IDFConstants.IMediationFileNames.CNUM_ESTABCAUSE_QCI_FILENAME.equalsIgnoreCase(fileName))
				{
					cNum_EstabCause_QCIAggValues=populateValues(valueRow, cNum_EstabCause_QCIAggValues);
				}else if(IDFConstants.IMediationFileNames.CNUM_FILENAME.equalsIgnoreCase(fileName))
				{
					cNumAggValues=populateValues(valueRow, cNumAggValues);
				}else if(IDFConstants.IMediationFileNames.CNUM_QCI_FILENAME.equalsIgnoreCase(fileName))
				{
					cNum_QCIAggValues=populateValues(valueRow, cNum_QCIAggValues);
				}else if(IDFConstants.IMediationFileNames.CNUM_TARGETEARFCNDL_FILENAME.equalsIgnoreCase(fileName))
				{
					cNum_TargetEarfcnDlAggValues=populateValues(valueRow, cNum_TargetEarfcnDlAggValues);
				}else if(IDFConstants.IMediationFileNames.CNUM_TCID_HOCAUSE_FILENAME.equalsIgnoreCase(fileName))
				{
					cNum_tcID_HoCauseAggValues=populateValues(valueRow, cNum_tcID_HoCauseAggValues);
				}  
				
			}
			StringBuffer sb=new StringBuffer();
			double expression1Value=populateExpression1(cNum_tcID_HoCauseAggValues,cNum_TargetEarfcnDlAggValues, IDFConstants.IMediationFileNames.CNUM_TCID_HOCAUSE_FILENAME, IDFConstants.IMediationFileNames.CNUM_TARGETEARFCNDL_FILENAME );
			context.write(key, new Text(String.valueOf(expression1Value)));
		}
	}

	public String buildValue(double[] values, String fileName){
		if(null!=values && values.length!=0)
		{
		String endResult=fileName+IDFConstants.FILENMAE_OUPUT_VALUE_SEPARATOR;
		StringBuffer value=new StringBuffer();
		for (double d : values) {
			value.append(d+IDFConstants.VALUE_SEPERATOR);
		}
		endResult+=value.substring(0, value.indexOf(IDFConstants.VALUE_SEPERATOR));
		return endResult;
		}return "";
	}
	
	public double[] populateValues(String value, double[] previousValues) {
		// 0_37.5_23_9
		String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(
				value, IDFConstants.DATA_FILE_DELIMITER);
		if (previousValues == null) {
			previousValues = new double[tokens.length];
		}
		for (int i = 0; i < tokens.length; i++) {
			try {
				previousValues[i] += Double.parseDouble(tokens[i]);
			} catch (NumberFormatException e) {
				previousValues[i] += 0;
			}
		}
		return previousValues;
	}
	
	public double populateExpression1(double[] cNum_tcID_HoCauseAggValues,double[] cNum_TargetEarfcnDlAggValues , String firstFileName, String secondFileName ){
		
		double endResult=0;
		
		Integer[] firstCol=emitingCols.get(firstFileName);
		Integer[] secondCol=emitingCols.get(secondFileName);
		endResult=cNum_tcID_HoCauseAggValues[getColIdx(firstCol, 56)]+cNum_TargetEarfcnDlAggValues[getColIdx(secondCol, 72)];
		
		return endResult;
	}
	
	public int getColIdx(Integer[] cols, int value)
	{
		for (int i = 0; i < cols.length; i++) {
			if(value==cols[i]){
				return i;
			}
		}
		return 0;
	}
}
