package com.otsi.reducer.calc;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.otsi.constants.IDFConstants;

public class CnumCalCombiner<K1 extends WritableComparable<?>,
V1 extends Writable> extends Reducer<Text, Text, Text, Text> {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void reduce(Text key, Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		double[] outputValues=null;
		String fileName=null;
		for (Text text : values) {
			String outputValue=text.toString();
			String[] tokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(outputValue, IDFConstants.FILENMAE_OUPUT_VALUE_SEPARATOR);
			fileName=tokens[0];
			outputValues=populateValues(tokens[1], outputValues);
			Integer.parseInt(fileName+outputValues);
		}
		context.write(key, new Text(buildValue(outputValues, fileName)) );
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
