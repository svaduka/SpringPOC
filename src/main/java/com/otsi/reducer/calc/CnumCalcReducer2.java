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

public class CnumCalcReducer2 extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		
	}

	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		
	}
	
	
}
