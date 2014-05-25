package com.otsi.preprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.HashedMap;

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
