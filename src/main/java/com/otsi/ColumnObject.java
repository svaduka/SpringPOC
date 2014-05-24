package com.otsi;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.otsi.constants.IDFConstants;

/**
 * ColumnObject class will store the column information of the meta file
 * 
 * @author DEV TEAM
 * @version R3
 * 
 */
public class ColumnObject {

	/**
	 * @param args
	 */

	private String columnName;
	private String dataType;
	private String avroDataType;
	private Integer length;
	private Integer precision;
	private Integer columnId;// Position of the column in the data file
	private String format;
	private String primaryKey;
	private String sourceSystemTimeZone = null;
	private Map<String, String> propertyMapper=new HashMap<String, String>();

	private Integer scale;
	/**
	 * @return the format
	 */
	public String getFormat() {
		return format;
	}

	/**
	 * @param format
	 *            the format to set
	 */
	public void setFormat(String format) {
		this.format = format;
		if(!IDFConstants.IJSON.IGNORE_SOME_FIELDS)
		{
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_FORMAT, format);
		}
	}

	public Integer getScale() {
		return scale;
	}

	public void setScale(Integer scale) {
		this.scale = scale;
		if(scale!=null){
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_SCALE, String.valueOf(this.scale));
		}
	}

	/**
	 * @return the primaryKey
	 */
	public String getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * @param primaryKey
	 *            the primaryKey to set
	 */
	public void setPrimaryKey(String primaryKey) {
		if(!IDFConstants.IJSON.IGNORE_SOME_FIELDS)
		{
		propertyMapper.put(IDFConstants.IJSON.PROP_PRIMARY_KEY,primaryKey);
		}
		this.primaryKey = primaryKey;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName
	 *            the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_NAME, columnName);
	}

	/**
	 * @return the dataType
	 */
	public String getDataType() {
		return dataType;
	}

	/**
	 * @param dataType
	 *            the dataType to set
	 */
	public void setDataType(String dataType) {
		if(null!=dataType){
			this.dataType = dataType;
		}else{
			this.dataType=IDFConstants.IDataTypes.DT_DB_VARCHAR;
		}
		if(!IDFConstants.IJSON.IGNORE_SOME_FIELDS){
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_TYPE, this.dataType);
		}
		setAvroDataType(dataType);
	}

	/**
	 * @return the length
	 */
	public Integer getLength() {
		return length;
	}

	/**
	 * @param length
	 *            the length to set
	 */
	public void setLength(Integer length) {
		if(null!=length){
		this.length = length;
		}else{
			this.length=IDFConstants.IDataTypes.DT_DEFAULT_LENGTH;
		}
		//ELement property is also set in precision please check
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_LENGTH, String.valueOf(this.length+(this.precision==null?0:this.precision)));
		
	}

	/**
	 * @return the precision
	 */
	public Integer getPrecision() {
		return precision;
	}

	/**
	 * @param precision
	 *            the precision to set
	 */
	public void setPrecision(Integer precision) {
		this.precision = precision;
		if(!IDFConstants.IJSON.IGNORE_SOME_FIELDS)
		{
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_PRECISION, String.valueOf(precision));
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_LENGTH, String.valueOf((this.length==null?0:this.length)+this.precision));
		}
	}

	/**
	 * @return the columnId
	 */
	public Integer getColumnId() {
		return columnId;
	}

	/**
	 * @param columnId
	 *            the columnId to set
	 */
	public void setColumnId(Integer columnId) {
		this.columnId = columnId;
		if(!IDFConstants.IJSON.IGNORE_SOME_FIELDS)
		{
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_COLUMN_ID, String.valueOf(columnId));
		}
	}

	/**
	 * @return the sourceSystemTimeZone
	 */
	public String getSourceSystemTimeZone() {
		return this.sourceSystemTimeZone;
	}

	/**
	 * @param sourceSystemTimeZone
	 *            the sourceSystemTimeZone to set
	 */
	public void setSourceSystemTimeZone(String sourceSystemTimeZone) {
		this.sourceSystemTimeZone = sourceSystemTimeZone;
		if(!IDFConstants.IJSON.IGNORE_SOME_FIELDS)
		{
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_TIMEZONE, sourceSystemTimeZone);
		}
	}


	@Override
	public String toString() {
		return "ColumnObject [columnName=" + columnName + ", dataType="
				+ dataType + ", length=" + length + ", precision=" + precision
				+ ", columnId=" + columnId + ", format=" + format
				+ ", primaryKey=" + primaryKey + ", sourceSystemTimeZone="
				+ sourceSystemTimeZone + "]";
	}

	
	public String toAvroFieldString()
	{
		return buildJsonString().toJSONString().toLowerCase();
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject buildJsonString(){
		JSONObject object=null;
		if(null!=propertyMapper && !propertyMapper.isEmpty())
		{
			object=new JSONObject();
			//We need to add 3 more fields + 1 type_schema_type
			for (Map.Entry<String, String> mapEntries : propertyMapper.entrySet()) {
				object.put(mapEntries.getKey(), mapEntries.getValue());
			}
			object.put(IDFConstants.IJSON.ELEMENT_MAPPING,buildMappingElement().get(IDFConstants.IJSON.ELEMENT_MAPPING));
			if(!isPrimaryKeyField()){
				if(this.avroDataType.equals(IDFConstants.IDataTypes.DT_DB_AVRO_VARCHAR))
				{
					object.put(IDFConstants.IJSON.ELEMENT_DEFAULT,IDFConstants.IJSON.STRING_DEFAULT_VALUE);
				}else{
					object.put(IDFConstants.IJSON.ELEMENT_DEFAULT,new Integer(0));
				}
				}
			
		}
		return object;
	}
	public boolean isPrimaryKeyField(){
		if(this.primaryKey!=null && this.primaryKey.equalsIgnoreCase(IDFConstants.BOOLEAN_STRING_Y)){
			return Boolean.TRUE;
		}
		return Boolean.FALSE;
	}
	@SuppressWarnings("unchecked")
	public JSONObject buildMappingElement(){
		JSONObject mappingObject=new JSONObject();
		boolean isPrimaryKey=isPrimaryKeyField();
		JSONObject jsonObject=new JSONObject();
		if(isPrimaryKey)
		{
			jsonObject.put(IDFConstants.IJSON.ELEMENT_TYPE, IDFConstants.IJSON.ELEMENT_VALUE_KEY);
		}else{
			jsonObject.put(IDFConstants.IJSON.ELEMENT_TYPE, IDFConstants.IJSON.ELEMENT_VALUE_COLUMN);	
		}
		jsonObject.put(IDFConstants.IJSON.ELEMENT_KEY_VALUE, "meta:"+this.getColumnName());
		mappingObject.put(IDFConstants.IJSON.ELEMENT_MAPPING, jsonObject);
		return mappingObject;
	}
	public String getAVRODataType(final String metaColumnDataType)
	{
		String dataType=IDFConstants.IDataTypes.DEFAULT_DATATYPE;
		if(null!=metaColumnDataType){
			String metaTypeUpper=metaColumnDataType.toUpperCase();
			if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_SMALLINT)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_SMALLINT;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_BIGINT)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_BIGINT;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_INTEGER)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_INTEGER;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_INT)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_INTEGER;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_DECIMAL)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_DECIMAL;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_CHAR)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_CHAR;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_VARCHAR)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_VARCHAR;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_DATE)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_DATE;
			}else if(metaTypeUpper.equalsIgnoreCase(IDFConstants.IDataTypes.DT_DB_TIMESTAMP)){
				dataType=IDFConstants.IDataTypes.DT_DB_AVRO_TIMESTAMP;
			}
		}
		return dataType;
	}
	
	@SuppressWarnings("unchecked")
	public JSONObject getDefaultValue(){
		JSONObject defaultObj=new JSONObject();
		if(getAvroDataType().equals(IDFConstants.IDataTypes.DT_DB_AVRO_VARCHAR))
		{
			if(this.columnName.equalsIgnoreCase(IDFConstants.IJSON.NAME_READER_WRITER_TYPE))
			{
				defaultObj.put(IDFConstants.IJSON.ELEMENT_DEFAULT,IDFConstants.IJSON.PROPERTY_TYPE_DEFAULT_VALUE);
			}else{
				defaultObj.put(IDFConstants.IJSON.ELEMENT_DEFAULT,IDFConstants.IJSON.STRING_DEFAULT_VALUE );
				
			}
		}else{
			defaultObj.put(IDFConstants.IJSON.ELEMENT_DEFAULT, IDFConstants.IJSON.VALUE_ZERO);
		}
		return defaultObj;
	}

	public String getAvroDataType() {
		return avroDataType;
	}

	public void setAvroDataType(String avroDataType) {
		String dataType=getAVRODataType(avroDataType);
		this.avroDataType = dataType;
		propertyMapper.put(IDFConstants.IJSON.ELEMENT_TYPE, dataType);
	}
}
