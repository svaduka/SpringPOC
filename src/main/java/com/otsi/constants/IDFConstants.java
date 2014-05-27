package com.otsi.constants;

public interface IDFConstants {
	
	
	//LTE_CNUM_SCHEMA FILE LOCATION
	public static final String SCHEMA_LTE_FILE_PARENT_DIR="hdfs:/data1/lte/files/schema";
	//Regular Constants
	public static final String CONTROL_FILE_DELIMITER="|";
	public static final String KEY_SCHEMA_NAME = "SCHEMA_NAME";
	public static final String KEY_IS_FIXED_SCHEMA = "KEY_IS_FIXED_SCHEMA";
	public static final String KEY_DELIMITER = "KEY_DELIMITER";
	public static final String EMPTY_STRING = "";
	public static final String BOOLEAN_STRING_N = "N";
	public static final String DATA_FILE_DELIMITER = ",";
	public static final String FILENAME_FIELDS_SEPERATOR = "--";
//	public static final String REPO_LOCATION_FOR_META_FILES = "/home/kf416913/LTE/META";
	public static final String REPO_LOCATION_FOR_META_FILES ="/root/OTSI/DATA/MEDIATION_FILES/SCHEMA/Schema";
	public static String LOCATION_SCHEMA_FILE_NAME="/root/OTSI/DATA/MEDIATION_FILES/SCHEMA/managedschema/Managed_Schemas.avsc";
//	public static String LOCATION_SCHEMA_FILE_NAME="/home/cloudera/SAIWS/OTSI/MEDIATION_FILES/SCHEMA/managedschema/Managed_Schemas.avsc";
//	public static String LOCATION_SCHEMA_FILE_NAME="/home/kf416913/managedschema/Managed_Schemas.avsc";
//	public static String LOCATION_SCHEMA_FILE_NAME="/data1/eclipse/SAIWS/avscFiles/manage_schema.avsc";
	public static String STATIC_FIXED_WIDTH_SCHEMA="{\"type\":\"record\",\"name\":\"FIXED_TABLE\",\"namespace\":\"com.optum.df.avro\",\"fields\":[{\"name\":\"FIXED_COL\",\"type\":\"string\",\"default\":\"NONE\"}]}";
	
	
	interface IFile{
		public static final String EXT_META_FILE=".meta";
		public static final String EXT_CTL_FILE=".ctl";
		public static final String EXT_DAT_FILE=".dat";
		public static final String EXT_CSV_DAT_FILE=".CSV";
		public static final String EXT_SCHEMA_FILE=".schema";
		public static final String FILENAME_TYPE_SEPARATOR = ".";
		
		
		//START CONTROL FILE PROPERTY NAME INDICATORS
		public static final String CNTRL_FILE_PROP_SOURCE_NAME = "Source";
		public static final String CNTRL_FILE_PROP_SCHEMA_NAME = "Schema";
		public static final String CNTRL_FILE_PROP_TABLE_NAME = "Table_Name";
		public static final String CNTRL_FILE_PROP_RECORD_COUNT_NAME = "Record_Count";
		public static final String CNTRL_FILE_PROP_EXT_TS="CNTRL_FILE_EXT_TS"; // 8 in Control file
		public static final String KEY_PROP_DELIMITER="delimeter"; //property used to find what is the delimiter in control file
		public static final String CONTROL_FILE_FIXED_WIDTH_FILE_INDICATOR = "FIXED";
		//END CONTROL FILE PROPERTY NAME INDICATORS
		public static final String META_FILE_DELIMITER = ",";
	}
	
	interface IExceptionMsgs{
		public final static String CONFIGURATION_NOT_FOUND="No Configuration Found";
	}
	
	interface IJSON{
		public static String KEY_NAME="\"name\"";
		public static String KEY_TYPE="\"type\"";
		public static String KEY_DEFAULT="\"default\"";
		public static String KEY_LENGTH="\"length\"";
		public static String KEY_MAPPING="\"mapping\"";
		public static String ZERO_STRING="\"0\"";
		public static String VALUE_STRING="\"string\"";
		public static String VALUE_NUMBER="\"number\"";
		public static String DEFAULT_VALUE_NULL="\"null\"";
		public static String KEY_SEPERATOR=",";
		public static String KEY_VALUE_SEPERATOR=":";
		public static String DT_SEPERATOR=",";
		public static String EMPTY_STRING="\"\"";
		public static String VALUE_KEY = "\"key\"";
		public static String KEY_VALUE = "\"value\"";
		public static String VALUE_COLUM = "\"column\"";
		public static String VALUE_NULL = "null";
		public static String ELEMENT_MAPPING = "mapping";
		public static String ELEMENT_VALUE_COLUMN = "column";
		public static String ELEMENT_TYPE = "type";
		public static String ELEMENT_VALUE_KEY = "key";
		public static String ELEMENT_KEY_VALUE = "value";

		public static String ELEMENT_NAME = "name";
		public static String ELEMENT_LENGTH = "length";
		public static String PROPERTY_TYPE = "TYPE";
		public static String PROPERTY_TYPE_DEFAULT_VALUE = "W";
		public static String ELEMENT_FORMAT="format";
		public static String PROP_PRIMARY_KEY = "primary_key";
		public static String ELEMENT_PRECISION = "precision";
		public static String ELEMENT_COLUMN_ID = "columnId";
		public static String ELEMENT_TIMEZONE = "timezone";
		public static String ELEMENT_DEFAULT = "default";
		public static String STRING_DEFAULT_VALUE = "NONE";
		public static String VALUE_ZERO = "0";
		public static String NAME_EFFECTIVE_DATE = "EffectiveDate";
		public static String NAME_SEQUENCE_NUMBER = "SequenceNumber";
		public static String NAME_READER_WRITER_TYPE = "SchemaType";
		public static String NAME_WATERMARK_ID = "WaterMark";
		public static String ELEMENT_SCALE = "scale";
		public static String ELEMENT_POSITION = "pos";
		public static final boolean IGNORE_SOME_FIELDS = Boolean.FALSE;
	}
	public static String BOOLEAN_STRING_Y="Y";
	
	interface IDataTypes {
//		public static String DT_VARCHAR		=	"VARCHAR";
//		public static String DT_CHAR		=	"CHAR";
//		public static String DT_INTEGER		=	"INT";
//		public static String DT_BINARY		=	"BINARY";
//		public static String DT_NUMBER		=	"NUMBER";
//		public static String DT_TIMESTAMP	=	"TIMESTAMP";
//		public static String DT_JSON_NUMBER = "long";
//		public static String DT_JSON_STRING = "string";
//		public static String DT_JSON_BOOLEAN = "boolean";
//		public static String DT_JSON_ARRAY = "array";
//		public static String DT_JSON_VALUE = "Value";
//		public static String DT_JSON_OBJECT = "Object";
//		public static String DT_JSON_WHITESPACE = "Whitespace";
//		public static String DT_JSON_NULL = "null";
//		public static CharSequence DT_JSON_INT = "INT";
		
		//Mail sent from Mike for data types
		public static int DT_DEFAULT_LENGTH	=	25;
		public static String DT_DB_SMALLINT="SMALLINT";
		public static String DT_DB_BIGINT="BIGINT";
		public static String DT_DB_INTEGER="INTEGER";
		public static String DT_DB_INT="INT";
		public static String DT_DB_DECIMAL="DECIMAL";
		public static String DT_DB_CHAR="CHAR";
		public static String DT_DB_VARCHAR="VARCHAR";
		public static String DT_DB_DATE="DATE";
		public static String DT_DB_TIMESTAMP="TIMESTAMP";
		public static String DT_DB_DATE_TIME="DATETIME";
		
		//CORRESTPONDING AVRO DATA TYPES
		public static String DEFAULT_DATATYPE="string";
		public static String DT_DB_AVRO_SMALLINT="int";
		public static String DT_DB_AVRO_BIGINT="int";
		public static String DT_DB_AVRO_INTEGER="int";
		public static String DT_DB_AVRO_DECIMAL="double";
		public static String DT_DB_AVRO_CHAR="string";
		public static String DT_DB_AVRO_VARCHAR="string";
		public static String DT_DB_AVRO_DATE="string";
		public static String DT_DB_AVRO_TIMESTAMP="string";
		public static String DT_DB_AVRO_DATE_TIME="string";
		
	}
	
	interface ITableSchemaDefinition{
		public static final String FIELD_DATA_SOURCE="DATA_SOURCE";
		public static final String FIELD_TABLE_NAME="TABLE_NAME";
		public static final String FIELD_SCHEMA_STRING="SCHEMA_STRING";
		public static final String FIELD_SCHEMA_DATE="SCHEMA_DATE";
		public static final String FIELD_CREATE_DATE="CREATE_DATE";
		public static final String FIELD_VERSION_NUMBER="VERSION_NUMBER";
		
	}
	
	//Calculation Constants
	public static final String PROP_KEY_VAL_SEPARATOR="=";
	
	interface IConfParameters{
		public static final String PROP_EXPRESSION_COL_JSON="JSON_EXPRESSION_COLUMN_IDENTIFIER";
		public static final String PROP_KEY_LEVEL_JSON = "JSON_EXPRESSION_AVAILABLE_LEVELS";
		public static final String KEY_ALL_MEDIATION_SCHEMA = "MEDIATION_SCHEMA";
	}

}
