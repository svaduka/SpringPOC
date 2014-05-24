package com.otsi.util;

import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetExistsException;
import org.kitesdk.data.DatasetReader;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Formats;

import com.otsi.TableMetaData;
import com.otsi.constants.IDFConstants;

public class KiteUtil {
	private static Log logger=LogFactory.getLog(KiteUtil.class);
	private static KiteUtil oKiteUtil;
	private DatasetRepository hiveRepo;
	private DatasetRepository hdfsRepo;
	private KiteUtil(){
		
	}
	
	public static KiteUtil getKiteUtil(){
		if(null==oKiteUtil){
			oKiteUtil=new KiteUtil();
		}
		return oKiteUtil;
	}
	
	public DatasetRepository getRepo(final String repoURL)
	{
		DatasetRepository hdfsRepo=DatasetRepositories.open(repoURL);
		return hdfsRepo;
	}

	public DatasetRepository getHiveRepo() {
		return hiveRepo;
	}

	public void setHiveRepo(String hiveRepoURL) {
		this.hiveRepo = getRepo(hiveRepoURL);
	}

	public DatasetRepository getHdfsRepo() {
		return hdfsRepo;
	}

	public void setHdfsRepo(String hdfsRepoURL) {
		this.hdfsRepo = getRepo(hdfsRepoURL);
	}
	/**
	 * 
	 * @param schema
	 * @param URIPattern
	 * @return TRUE/FALSE whether table created or not. It even Check whether table already exist or not
	 */
	public boolean createTable(final Schema schema){
		if(null==schema){
			return Boolean.FALSE;
		}
		final String tableName=schema.getName();
		if(null!=hiveRepo)
		{
			boolean isExist=hiveRepo.exists(tableName);
			if(!isExist)
			{
				DatasetDescriptor descriptor;
					descriptor = new DatasetDescriptor.Builder().schema(schema).build();
					hiveRepo.create(tableName, descriptor);
					return Boolean.TRUE;
			}else{
				return Boolean.TRUE;
			}
		}
		return Boolean.FALSE;
	}
	
	/**
	 * 
	 * @param dataSchema
	 * @param repositoryURL
	 * @param managedSchema
	 * @return TRUE/FALSE indicating whether we successfully register schema or not
	 */
	public boolean registerSchema(TableMetaData oTableMetaData)
	{
		final String METHOD_NAME="-----registerSchema(TableMetaData oTableMetaData)-----";
		logger.info("ENTRY METHOD:"+METHOD_NAME);
		Schema managedSchema=oTableMetaData.getManagedSchema();
		boolean isCreated=createTable(managedSchema);
		if(isCreated)
		{
			Dataset<GenericRecord> inputData=null;
			DatasetWriter<GenericRecord> writer=null;
			final String managedTableName=managedSchema.getName();
			inputData=hiveRepo.load(managedTableName);
			boolean exist=checkSchemaExist(oTableMetaData);
			if(!exist)
			{
			Schema dataSchema=oTableMetaData.getSchema();	
			Date currentDate = new Date();
			GenericRecordBuilder builder = new GenericRecordBuilder(managedSchema);
			builder.set(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_DATA_SOURCE).name(),dataSchema.getName());
			builder.set(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_TABLE_NAME).name(),dataSchema.getName());
			builder.set(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_SCHEMA_STRING).name(),dataSchema.toString());
			builder.set(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_SCHEMA_DATE).name(),currentDate.toString());
			builder.set(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_CREATE_DATE).name(),currentDate.toString());
			builder.set(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_VERSION_NUMBER).name(),String.valueOf(generateVersion(oTableMetaData)));
			GenericRecord record = builder.build();
			try {
				writer = inputData.newWriter();
				if(!writer.isOpen())
				{
				writer.open();
				}
				writer.write(record);
			}finally{
				if(null!=writer)
				{
					writer.close();
				}
			}
		}
			return Boolean.TRUE;
		}
		logger.info("EXIT METHOD:"+METHOD_NAME);
		return Boolean.FALSE;
	}
	
	public boolean checkSchemaExist(TableMetaData oTableMetaData){
		if(null!=hiveRepo)
		{
			//again check whether data exist
			final String managedSchemaTableName=oTableMetaData.getManagedSchemaTable();
			boolean isExist=hiveRepo.exists(managedSchemaTableName);
			if(!isExist){
				return Boolean.FALSE;
			}
			Dataset<GenericRecord> dataRecords=hiveRepo.load(managedSchemaTableName); //loading data
			DatasetReader<GenericRecord> reader = dataRecords.newReader();
			Schema dataFileSchema=oTableMetaData.getSchema();
			Schema managedSchema=oTableMetaData.getManagedSchema();
			final String schemaColumnName=managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_SCHEMA_STRING).name();
			try{
				reader.open();
				Schema oCheckSchema=null;
				List<Field> compareFields=dataFileSchema.getFields();
				for (GenericRecord dataRecord : reader) {
					String schema=(String)dataRecord.get(schemaColumnName);
					oCheckSchema=new Schema.Parser().parse(schema);
					if(!oCheckSchema.getFullName().equalsIgnoreCase(dataFileSchema.getFullName())){
						continue;
					}
					List<Field> fields=oCheckSchema.getFields();
					if(DFUtil.compareLists(fields, compareFields)){
						return Boolean.TRUE;
					}
				}
			}finally{
				
			}
		}
		return Boolean.FALSE;
	}
	
	
	/**
	 * 
	 * @param repo
	 * @param URL
	 * @param tableName
	 * @param schemaColumnName
	 * @param oSchema
	 * @return Calculate the Version number for the Schema
	 */
	public Long generateVersion(TableMetaData oTableMetaData){
		long version=1l;
	
		if(null!=hiveRepo)
		{
			//again check whether data exist
			Schema managedSchema=oTableMetaData.getManagedSchema();
			final String managedTableName=managedSchema.getName();
			boolean isExist=hiveRepo.exists(managedTableName);
			if(!isExist){
				return version;
			}
			Dataset<GenericRecord> dataRecords=hiveRepo.load(managedTableName); //loading data
			DatasetReader<GenericRecord> reader = dataRecords.newReader();
			Schema oDataSchema=oTableMetaData.getSchema();
			try{
				reader.open();
				Schema oCheckSchema=null;
				for (GenericRecord dataRecord : reader) {
					String schema=(String)dataRecord.get(managedSchema.getField(IDFConstants.ITableSchemaDefinition.FIELD_SCHEMA_STRING).name());
					oCheckSchema=new Schema.Parser().parse(schema);
					if(!oCheckSchema.getFullName().equalsIgnoreCase(oDataSchema.getFullName())){
						continue;
					}
					version++;
				}
			}finally{
				reader.close();
			}
		}
		return version;
	}

	public void convertDataFileToAvroFile(TableMetaData tableMetaData) {
		 Schema oSchema=tableMetaData.getSchema();// Data file schema
		 DatasetRepository dataHDFSRepo=getRepo("repo:"+tableMetaData.getHdfsRootURL());
		 final String tableName=tableMetaData.getParentDir();
		 Dataset<GenericRecord> dataFileData=dataHDFSRepo.load(tableName);
		 DatasetReader<GenericRecord> reader = dataFileData.newReader();
		 DatasetDescriptor destinationAvroDescriptor=new DatasetDescriptor.Builder().schema(tableMetaData.getSchema()).build();
		 if(!hdfsRepo.exists(tableName))
			 {
				 hdfsRepo.create(tableName, destinationAvroDescriptor);
			 }
			 Dataset<GenericRecord> destinationFolder= hdfsRepo.load(tableName);
			 DatasetWriter<GenericRecord> writer=null;
			    try {
			      reader.open();
			      writer=destinationFolder.newWriter();
			      writer.open();
			      for (GenericRecord user : reader) {
			    	  String line=user.toString();
			    	  String data[]=getFixedData(line, tableMetaData.getSchema());
			    	  writer.write(user);
			      }
			    } finally {
			      reader.close();
			      if(null!=writer)
			      {
			    	  writer.close();
			      }
			      
			    }
	}
	
	
	/**
	 * 
	 * @param oSchema
	 * @return Boolean TRUE/FALSE.
	 * It will update schema on table.
	 * If schema is update then it will update otherwise it will give us exception currently we are catching exception and just mentioning that schema is not compatable
	 */
	public boolean updateSchema(TableMetaData oTableMetaData) {
		boolean isCompatable=Boolean.TRUE;
		DatasetDescriptor.Builder descriptorBuilder=null;
		Schema oSchema=oTableMetaData.getSchema();
		final String datasetName=oSchema.getName();
		try{
			boolean isExists=hiveRepo.exists(datasetName); //Test whether exist or not
			if(isExists){
				Dataset<GenericRecord> tableData=hiveRepo.load(datasetName);
				descriptorBuilder=new DatasetDescriptor.Builder(tableData.getDescriptor()).schema(oSchema);
				descriptorBuilder.format(Formats.AVRO);
				hiveRepo.update(datasetName, descriptorBuilder.build());
			}
		}catch(org.kitesdk.data.IncompatibleSchemaException i){
			isCompatable=Boolean.FALSE;
			i.printStackTrace();
		}
		return isCompatable;
	}
	
	public boolean createDatasetDescriptor(TableMetaData tableMetaData)
 {
		
		final String METHOD_NAME="-----createDatasetDescriptor(TableMetaData tableMetaData)-----";
		logger.info("ENTRY METHOD:"+METHOD_NAME);
		DatasetDescriptor dataDescriptor = null;
		Schema oSchema = tableMetaData.getSchema();// Data file schema
//		DatasetRepository dataHDFSRepo = getRepo("repo:"+ tableMetaData.getHdfsRootURL());
//		if(tableMetaData.getDatFileDelimiter().equals(IDFConstants.IFile.CONTROL_FILE_FIXED_WIDTH_FILE_INDICATOR)){
//			oSchema=new Schema.Parser().parse(IDFConstants.STATIC_FIXED_WIDTH_SCHEMA);
//		}
		dataDescriptor = new DatasetDescriptor.Builder()
				.schema(oSchema)
				.format(Formats.AVRO)
				.build();
		try{
			if(!hdfsRepo.exists(tableMetaData.getTableName()))
			{
				hdfsRepo.create(tableMetaData.getTableName(), dataDescriptor);
			}
		}catch(DatasetExistsException de){
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
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
				logger.info("Data for  StartIdx: " + iStartIdx + " :iEndIdx +"
						+ iEndIdx + " is :" + temp);
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
}
