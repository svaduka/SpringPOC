package com.otsi.reducer.calc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.otsi.constants.IDFConstants;

public class CnumCalcReducer extends Reducer<Text, Text, Text, Text> {
	
	public String[] levelNames;
	public Map<String, Map<Integer, String>> mapColumnNames;
	public Map<String, Schema> allSchemas;
	@SuppressWarnings("rawtypes")
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		Configuration conf=context.getConfiguration();
		String levelJson=conf.get(IDFConstants.IConfParameters.PROP_KEY_LEVEL_JSON);
		try {
			JSONArray array=new JSONArray(levelJson);
			levelNames=new String[array.length()];
			for (int i = 0; i < array.length(); i++) {
				levelNames[i] = array.getString(i);
			}
			generateSchema(conf);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		
		for (Text text : values) {
			String value=text.toString();
			String[] fileWithTokens=value.split("\t");
			String fileName=fileWithTokens[0];
			String valWithColIdx=fileWithTokens[1];
		}
	}
	
	private void generateSchema(Configuration conf){
		String completeSchemas=conf.get(IDFConstants.IConfParameters.KEY_ALL_MEDIATION_SCHEMA);
		try {
			JSONObject json=new JSONObject(completeSchemas);
			allSchemas=new HashMap<String, Schema>();
			for (String levelName : levelNames) {
				String schemaString=json.getString(levelName);
				Schema oSchema=new Schema.Parser().parse(schemaString);
				allSchemas.put(levelName, oSchema);
			}
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
