package com.otsi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * TableObject class have the information about the Source_name and table Name
 * and all columns related to that methods
 * 
 * @author DEV TEAM
 * @version R3
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TableObject {
	String sourceName;
	String owner;
	String tableName;
	private List<ColumnObject> OrderedColumnObjects=new ArrayList<ColumnObject>();


	LinkedHashMap columnList = new LinkedHashMap(); // This list contain all the
													// column in table and its
													// populated from schema
													// file

	/**
	 * @return the owner
	 */
	public String getOwner() {
		return owner;
	}

	/**
	 * @param owner
	 *            the owner to set
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the columnList
	 */
	public LinkedHashMap getColumnList() {
		return columnList;
	}

	/**
	 * @param columnList
	 *            the columnList to set
	 */
	public void setColumnList(LinkedHashMap columnList) {
		this.columnList = columnList;
	}

	public void putColumnList(String columName, ColumnObject columnObject) {
		getOrderedColumnObjects().add(columnObject);
		this.columnList.put(columName, columnObject);
	}

	/**
	 * @return the sourceName
	 */
	public String getSourceName() {
		return sourceName;
	}

	public List<ColumnObject> getOrderedColumnObjects() {
		return OrderedColumnObjects;
	}

	public void setOrderedColumnObjects(List<ColumnObject> orderedColumnObjects) {
		OrderedColumnObjects = orderedColumnObjects;
	}

	/**
	 * @param sourceName
	 *            the sourceName to set
	 */
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TableObject [sourceName=" + sourceName + ", owner=" + owner
				+ ", tableName=" + tableName + ", columnList=" + columnList
				+ "]";
	}

	/** 
	 * 
	 * @return Avro Schema for given Column.
	 */
	public String getAvroSchema() {
		String jsonStr = "{ \"namespace\": \"com.optum.df.avro\", \"type\": \"record\", \"name\": \""
				+ this.tableName.toUpperCase() + "\" ,";
		ColumnObject currentColumn;
		String fieldsStr = "";

		Iterator<ColumnObject> ColListIt = this.columnList.values().iterator();

		while (ColListIt.hasNext()) {
			currentColumn = (ColumnObject) ColListIt.next();
			fieldsStr += currentColumn.toAvroFieldString() + ",";
		}

		if (fieldsStr.length() > 1)
			jsonStr += " \"fields\" : [ " + fieldsStr.substring(0, fieldsStr.length() - 1) + "] }";
		return jsonStr;
	}
}
