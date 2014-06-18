package com.otsi.preprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.collections.map.HashedMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.otsi.constants.IDFConstants;

public class PreprocessCalcs {

	/**
	 * 
	 * @param iCalcFileProperties
	 * @return contains calculation process for calc table destinationColumnName
	 *         = (TABLENAME.COLUMN_NAME+TABLENAME2.COLUMN_NAME)*(TABLENAME.
	 *         COLUMN_NAME/TABLENAME3.COLUMN_NAME3) TODO Include Column_Name
	 *         POSITIONS
	 */
	
	public static String getJsonString(final String iCalcFileProperties,Map<String, Schema> schemas)
	{
		Map<String, Set<Integer>> calcUsages=getIdxColumns(iCalcFileProperties,schemas);
		JSONArray array=new JSONArray();
		JSONObject object=null;
		for (Map.Entry<String, Set<Integer>> e : calcUsages.entrySet()) {
			object=new JSONObject();
			object.put(e.getKey(), e.getValue());
			array.add(object);
		}
		return array.toJSONString();
	}
	/**
	 * Send the schema for mediation schema
	 */
	public static Map<String, Set<Integer>> getIdxColumns(final String iCalcFileProperties, Map<String, Schema> allMetas)
	{
		Map<String, Set<String>> calcUsages=getTablesAndColumnsList(iCalcFileProperties);
		Map<String, Set<Integer>> idxColumns=new HashedMap();
		for (Map.Entry<String, Set<String>> calcUsage : calcUsages.entrySet()) {
			String key=calcUsage.getKey();
			Set<String> values=calcUsage.getValue();
			Schema mergedSchema=allMetas.get(key);
			Set<Integer> idx=new HashSet<Integer>();
			if(null!=values && !values.isEmpty())
			{
			for (String colNames : values) {
				Field f=mergedSchema.getField(colNames);
				int schemaPos=Integer.parseInt(f.getProp(IDFConstants.IJSON.ELEMENT_POSITION));
				idx.add(schemaPos);
			}
			idxColumns.put(key, idx);
			}
		}
		
		return idxColumns;
		
	}
	public static Map<String, Set<String>> getTablesAndColumnsList(
			final String iCalcFileProperties) {
		Map<String, Set<String>> completeTableList = new HashedMap();
		File f = new File(iCalcFileProperties);
		if (f.exists()) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(f));
				String line = null;
				String expression = null;
				String[] tokens = null;
				while ((line = reader.readLine()) != null) {
					tokens = line.split(IDFConstants.PROP_KEY_VAL_SEPARATOR);
					expression = tokens[1];
					String regex = "(?:[^+*/%]|\\\\.)+";
					// step 1: split with operator
					Pattern p = Pattern.compile(regex);
					Matcher m = p.matcher(expression);
					while (m.find()) {
						String literal = m.group().replaceAll("[(|)]", "");
						System.out.println(literal);
						String[] tableColumn = literal.split("\\W");
						if (completeTableList.get(tableColumn[0].toUpperCase()) == null) {
							Set<String> columns = new HashSet<String>();
							columns.add(tableColumn[1].trim().toUpperCase()
									.replaceAll("\\W", "_"));
							completeTableList.put(tableColumn[0].trim()
									.toUpperCase(), columns);
						} else {
							completeTableList.get(tableColumn[0].toUpperCase())
									.add(tableColumn[1].trim().toUpperCase()
											.replaceAll("\\W", "_"));
						}

					}
				}
			} catch (IOException o) {
				o.printStackTrace();
			}
		}
		return completeTableList;

	}
	public static void main(String[] args) {
		Map<String, Set<String>> completeTableList=getTablesAndColumnsList("/home/cloudera/SAIWS/OTSI/CNUM_END_CAL_TABLE.csv");
		System.out.println(completeTableList);
	}

}
