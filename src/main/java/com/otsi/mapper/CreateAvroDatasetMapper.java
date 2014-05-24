


package com.otsi.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.otsi.constants.IDFConstants;

public class CreateAvroDatasetMapper extends
		Mapper<LongWritable, Text, NullWritable, NullWritable> {
	
	Schema schema=null;
	boolean isFixed=Boolean.FALSE;
	String dataFileDelimiter=null;
	@SuppressWarnings("rawtypes")
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf=context.getConfiguration();
		final String schemaString=conf.get(IDFConstants.KEY_SCHEMA_NAME);
		if(null!=schemaString){
			schema=new Schema.Parser().parse(schemaString);
		}else{
			throw new NumberFormatException("Schema is empty"+conf);
		}
		isFixed=conf.getBoolean(IDFConstants.KEY_IS_FIXED_SCHEMA, Boolean.FALSE);
		if(!isFixed)
		{
			dataFileDelimiter=new Character((char)conf.getInt(IDFConstants.KEY_DELIMITER, 0)).toString();
		}
		
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		//Forget Key
		String inputLine=value.toString();
		if(null!=schema)
		{
		GenericRecordBuilder builder=new GenericRecordBuilder(schema);
		Field oField=null;
		int iPos=-1;
		String[] lineToken=null;
		if(!isFixed)
		{
			lineToken=StringUtils.splitByWholeSeparatorPreserveAllTokens(inputLine, dataFileDelimiter);
		}else{
			lineToken=getFixedData(inputLine, schema);
		}
		if(null!=lineToken && lineToken.length!=0)
		{
			final List<Field> oFields=schema.getFields();
			for (int i=0;i<oFields.size();i++) 
			{
					oField=oFields.get(i);
					iPos=oField.pos();
					if(oField.name().equals(IDFConstants.IJSON.NAME_READER_WRITER_TYPE)|| 
							oField.name().equals(IDFConstants.IJSON.NAME_EFFECTIVE_DATE)||
							oField.name().equals(IDFConstants.IJSON.NAME_SEQUENCE_NUMBER) ||
							oField.name().equals(IDFConstants.IJSON.NAME_WATERMARK_ID)){
						builder.set(IDFConstants.IJSON.NAME_READER_WRITER_TYPE, IDFConstants.IJSON.PROPERTY_TYPE_DEFAULT_VALUE);
						break;
					}
					builder.set(oField.name(), oField.schema().getType().getName().equalsIgnoreCase("LONG")?
												getLongValue(iPos, lineToken):oField.schema().getType().getName().equalsIgnoreCase("INT")?
															getIntValue(iPos, lineToken):oField.schema().getType().getName().equalsIgnoreCase("DOUBLE")?getDoubleValue(iPos, lineToken):getStringValue(iPos, lineToken));
					
					
				}
			GenericRecord record = builder.build();
			context.write(record, null);
		}else{
			Integer.parseInt("Schema is null");;
		}
		}
}
	
	
	
	

	public static String[] getFixedData(final String line, final Schema oSchema)
 {
		List<Field> fields = oSchema.getFields();
		String[] data = new String[fields.size()];
		String temp = null;
		for (Field field : fields) {
			int i = field.pos();
			int iEndIdx = getEndIdx(i, oSchema);
			if (iEndIdx != Integer.MIN_VALUE) {
				int iStartIdx = getStartIdx(i, oSchema);
				if(iEndIdx>line.length()){
					iEndIdx=line.length();
				}
				if(iStartIdx>line.length()){
					break;
				}
				temp = line.substring(iStartIdx, iEndIdx);
//				//logger.info("Data for  StartIdx: " + iStartIdx + " :iEndIdx +"
//						+ iEndIdx + " is :" + temp);
				data[i] = temp;
			}
		}

		return data;
	}
	public static int getStartIdx(int pos, Schema oSchema)
 {
		int iStartIdx = 0;
		List<Field> fields = oSchema.getFields();
		Field field = null;
		for (int i = 0; i < pos; i++) {
			field = fields.get(i);
			String lenght = field.getProp(IDFConstants.IJSON.ELEMENT_LENGTH);
			iStartIdx += Integer.parseInt(lenght);
		}
		return iStartIdx;
	}
	public static int getEndIdx(int pos, Schema oSchema)
	{
		int iEndIdx = 0;
		List<Field> fields = oSchema.getFields();
		Field field = null;
		for (int i = 0; i <= pos; i++) {
			field = fields.get(i);
			String lenght = field.getProp(IDFConstants.IJSON.ELEMENT_LENGTH);
			if(null!=lenght){
			iEndIdx += Integer.parseInt(lenght);
			}else{
				iEndIdx=Integer.MIN_VALUE;
			}
		}
		return iEndIdx;
	}
	
	
	private static Double getDoubleValue(int iPos, String[] lineTokens)  throws NumberFormatException
	{
		try {
			if (null != lineTokens && iPos < lineTokens.length) {
				if (lineTokens[iPos]!=null && !lineTokens[iPos].trim().equals(IDFConstants.EMPTY_STRING)) {
					return Double.parseDouble(lineTokens[iPos].trim());
				}else{
					return 0d;
				}
			}
		} catch (NumberFormatException e) {
			
		}
		return 0d;
		}
		
	public static int getIntValue(final int iPos, final String[] lineTokens)  throws NumberFormatException {
		try {
			if (null != lineTokens && iPos < lineTokens.length) {
				if (lineTokens[iPos]!=null && !lineTokens[iPos].trim().equals(IDFConstants.EMPTY_STRING)) {
					return Integer.parseInt(lineTokens[iPos].trim());
				}
			}
		} catch (NumberFormatException e) {
//			e.printStackTrace();
			//logger.error(iPos+" : "+lineTokens[iPos]);
//			throw e; 
		}
		return 0;
	}
public static long getLongValue(final int iPos,final String[] lineTokens) throws NumberFormatException{
	try{
	if(null!=lineTokens && iPos<lineTokens.length){
		if(lineTokens[iPos]!=null && !lineTokens[iPos].trim().equals(IDFConstants.EMPTY_STRING)){
			
			return Long.parseLong(lineTokens[iPos].trim());
		}
	}
	}catch(NumberFormatException e){
		//logger.error(iPos+" : "+lineTokens[iPos]);
//		throw e;
	}
	return 0;
}

public static String getStringValue(final int iPos,final String[] lineTokens){
	try{
	if(null!=lineTokens && iPos<lineTokens.length){
		if(lineTokens[iPos]!=null && !lineTokens[iPos].trim().equals(IDFConstants.EMPTY_STRING)){
			return lineTokens[iPos].trim();
		}
		else{
			return IDFConstants.IJSON.STRING_DEFAULT_VALUE;
		}
	}
	}catch(NumberFormatException e){
		//logger.error("Check Pos:"+iPos);
	}
	return IDFConstants.IJSON.STRING_DEFAULT_VALUE;
}
}