package com.otsi.reducer.calc;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.otsi.constants.IDFConstants;

public class CnumCalCombinerBak extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {

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
			sb.append(buildValue(cNum_DL_Layer_codewordAggValues, IDFConstants.IMediationFileNames.CNUM_DL_LAYER_CODEWORD_FILENAME));
			sb.append(buildValue(cNum_EstabCauseAggValues, IDFConstants.IMediationFileNames.CNUM_ESTABCAUSE_FILENAME));
			sb.append(buildValue(cNum_EstabCause_QCIAggValues, IDFConstants.IMediationFileNames.CNUM_ESTABCAUSE_QCI_FILENAME));
			sb.append(buildValue(cNumAggValues, IDFConstants.IMediationFileNames.CNUM_FILENAME));
			sb.append(buildValue(cNum_QCIAggValues, IDFConstants.IMediationFileNames.CNUM_QCI_FILENAME));
			sb.append(buildValue(cNum_TargetEarfcnDlAggValues, IDFConstants.IMediationFileNames.CNUM_TARGETEARFCNDL_FILENAME));
			sb.append(buildValue(cNumAggValues, IDFConstants.IMediationFileNames.CNUM_TCID_HOCAUSE_FILENAME));
//			context.write(key, value)
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
}
