package com.otsi.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.otsi.ColumnObject;
import com.otsi.TableObject;
import com.otsi.constants.IDFConstants;
import com.otsi.exception.DFException;

/**
 * 
 * @author Sainagaraju Vaduka
 * @Date: 05/07/2014
 * Used for HDFS related stuff;
 *
 */

// This is a singleton class to have access to HDFS.
public class HDFSUtil {
	static Log logger=LogFactory.getLog(HDFSUtil.class);
	private Configuration conf;//Mandatory stuff to have communication to HDFS.
	private static HDFSUtil oHDFSUtil;

	/**
	 * @param args
	 */
	private HDFSUtil(Configuration configuration){
		this.conf=configuration;
		
	}
	public static HDFSUtil getHDFSUtil(final Configuration configuration){
		if(null==configuration){
			logger.debug(IDFConstants.IExceptionMsgs.CONFIGURATION_NOT_FOUND);
			throw new DFException(IDFConstants.IExceptionMsgs.CONFIGURATION_NOT_FOUND);
		}
		if(null==oHDFSUtil)
		{
			oHDFSUtil=new HDFSUtil(configuration);
		}
		return oHDFSUtil;
	}
	
	public HDFSUtil overrideConfiguration(final Configuration conf){
		HDFSUtil oHDFsUtil=HDFSUtil.getHDFSUtil(conf);
		oHDFsUtil.conf=conf;
		return oHDFsUtil;
	}
	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}
	/**
	 * 
	 * @param iHDFSRootDir
	 * @return List of files from HDFS where we have all files 
	 */
	public List<String> getHDFSFiles(final String iHDFSRootDir)
	{
		Path rootDirectory=new Path(iHDFSRootDir); //Path representation of HDFS files
		FileSystem rootFileSystem;
		List<String> tablesInfo=new ArrayList<String>();
		try 
		{
			rootFileSystem = FileSystem.get(this.conf); //getting HDFS filesystem data
			FileStatus[] status=rootFileSystem.listStatus(rootDirectory); //Listing the files in the filesystem for a given path
			for (FileStatus fileStatus : status) //Iterating to get the directories 
			{ 
			tablesInfo.add(fileStatus.getPath().toString());
			}
		} catch (IOException e) {
			new DFException(e);
		}
		return tablesInfo;
	}
	
	public Map<String, List<String>> getHDFSFilesRecursively(final String iHDFSRootDir){

		System.out.println("----getHDFSFilesRecursively(final String iHDFSRootDir)-----");
		Path rootDirectory=new Path(iHDFSRootDir); //Path representation of HDFS files
		FileSystem rootFileSystem;
		Map<String, List<String>> completeHDFSfiles=new HashMap<String, List<String>>();
		try 
		{
			rootFileSystem = FileSystem.get(this.conf); //getting HDFS filesystem data
			FileStatus[] status=rootFileSystem.listStatus(rootDirectory); //Listing the files in the filesystem for a given path
			for (FileStatus fileStatus : status) //Iterating to get the directories 
			{ 
				String rootDir=fileStatus.getPath().getName();
				completeHDFSfiles.put(rootDir, getHDFSFiles(fileStatus.getPath().toString()));
			}
		} catch (IOException e) {
			new DFException(e);
		}
		System.out.println("completeHDFSfiles"+completeHDFSfiles);
		return completeHDFSfiles;
	
		
	}
	
	public String getContentFromHDFSURL(final String hdfsPathURL){
		System.out.println("-----getContentFromHDFSURL(final String hdfsPathURL) -----");
		System.out.println("hdfsPathURL:"+hdfsPathURL);
		StringBuffer sb=new StringBuffer();
		 try{
             Path pt=new Path(hdfsPathURL);
             FileSystem fs = FileSystem.get(this.conf);
             BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
             String line;
             while ((line=br.readLine()) != null){
            	 sb.append(line);
             }
     }catch(Exception e){
    	 new DFException(e);
     }
		return sb.toString();
	}
	public Schema convertMetaToAvroSchemaFile(String hdfsRepoURLForMetaFile, boolean isHDFS) {
		Schema schema=null;
		FileSystem fs = null;
		BufferedReader br=null;
		TableObject tableObject=null;
		 try{
			if(isHDFS)
			{
            Path pt=new Path(hdfsRepoURLForMetaFile);
            fs=FileSystem.get(this.conf);
            br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			}else{
				br=new BufferedReader(new FileReader(new File(hdfsRepoURLForMetaFile)));
			}
//            br.readLine();// This statement is to skip the first line 
            //SCHEMA_NAME|TABLE_NAME|COLUMN_NAME|DATA_TYPE|PRECISION|SCALE
    		String line=null;
    		String lastLine=null;
    		tableObject=new TableObject();
    		ColumnObject columnObject=null;
    		while((line = br.readLine()) != null) {
//    			System.out.println(line);
    			String[] lineToken= StringUtils.splitByWholeSeparatorPreserveAllTokens(line, IDFConstants.IFile.META_FILE_DELIMITER);
    			columnObject=new ColumnObject();
    			//Read row and insert data into columnObject class object 
    			columnObject.setColumnName(lineToken[2].replaceAll("\\W", "_").toUpperCase()); //To upper case since the avro schema by default setting the column names to UpperCase
    			columnObject.setDataType(lineToken[3]);
    			if (null!=lineToken[4] && !lineToken[4].isEmpty())
    			{
					try {
						columnObject
								.setPrecision(Integer.valueOf(lineToken[4]));
					} catch (NumberFormatException e) { //Some times the precision can be NULL
						columnObject.setPrecision(0);
					}
				}
    			if (null!=lineToken[5] && !lineToken[5].isEmpty())
    			{
    				try{
    				columnObject.setScale(Integer.valueOf(lineToken[5]));
    				}catch(NumberFormatException e){ //Sometimes the precision can be null
    					columnObject.setScale(0);
    				}
    			}
    			

    			//TODO Future version addition
//    			if (!lineToken[6].isEmpty())
//    				columnObject.setPrimaryKey(lineToken[6]);
    			
    			
    			//To store the single row into the TableObject
//    			System.out.println(columnObject);
    			tableObject.putColumnList(columnObject.getColumnName(), columnObject);
    			
    			lastLine=line;
    		} 
    		// Update owner and table Name
    		String[] lineToken= StringUtils.splitByWholeSeparatorPreserveAllTokens(lastLine, IDFConstants.IFile.META_FILE_DELIMITER);
    		tableObject.setOwner(lineToken[0]);
    		tableObject.setTableName(lineToken[1]);
    		
    }catch(Exception e)
    {
   	 new DFException(e);
    }finally{
    	if(null!=fs){
			try{
				fs.close();
				fs=null;
			}catch(IOException e){
				new DFException(e);
			}
		}
		if(null!=br){
			try{
				br.close();
				br=null;
			}catch(IOException e){
				new DFException(e);
			}
		}
    }
		 final String strSchema=tableObject.getAvroSchema();
		 schema=new Schema.Parser().parse(strSchema);		 
		return schema;
	}
	
public Map<String, List<Schema>> getMetaFiles(final String iHDFSLocationForMetaFiles)
{
	System.out.println("iHDFSLocationForMetaFiles"+iHDFSLocationForMetaFiles);
	Map<String, List<String>> hdfsFileRecursively=getHDFSFilesRecursively(iHDFSLocationForMetaFiles);
	System.out.println("Got Meta Files:"+hdfsFileRecursively);
	Map<String, List<Schema>> levelMetaFiles=new HashMap<String, List<Schema>>();
	for (Map.Entry<String, List<String>> metaFiles : hdfsFileRecursively.entrySet()) {
		String levelName=metaFiles.getKey();
		System.out.println("level Name:"+levelName);
		List<String> metaFileLocation=metaFiles.getValue();
		List<Schema> schemas=null;
		if(null!=metaFileLocation && !metaFileLocation.isEmpty()){
			schemas=new ArrayList<Schema>();
			Schema tempSchema=null;
			for (String metaLocation : metaFileLocation) {
				tempSchema=convertMetaToAvroSchemaFile(metaLocation,Boolean.TRUE);
				schemas.add(tempSchema);
			}
			
//			System.out.println(levelName.toUpperCase()+"::::"+schemas);
			levelMetaFiles.put(levelName.toUpperCase(), schemas);
		}
	}
	return levelMetaFiles;
}

public Schema getSchemaForMetaFile(final String mediationFileLocation, Map<String, List<Schema>> allMetaSchemas)
{
	if(null==allMetaSchemas || allMetaSchemas.isEmpty())
	{
		System.out.println("Run getMetaFiles before  getSchemaForMetaFile");
	}
	System.out.println("getSchemaForMetaFile(final String mediationFileLocation, Map<String, List<Schema>> allMetaSchemas)");
	final String header=getContentFromHDFSURL(mediationFileLocation,Boolean.TRUE);
	String[] tokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(header, IDFConstants.DATA_FILE_DELIMITER);
	String levelName=getLevelName(mediationFileLocation).toUpperCase();
	List<Schema> schemas=allMetaSchemas.get(levelName);
	JSONArray array=new JSONArray();
	JSONObject object=null;
	int iColIdx=-1;
	boolean isFieldPopulated=Boolean.FALSE;
	for (String columnName : tokens) {
		isFieldPopulated=Boolean.FALSE;
		iColIdx++;
		for (Schema schema : schemas) {
			if(columnName.equalsIgnoreCase("KEY7")){
				object=new JSONObject();
				object.put(IDFConstants.IJSON.ELEMENT_NAME, "KEY7");
				object.put(IDFConstants.IJSON.ELEMENT_TYPE, "string");
				object.put(IDFConstants.IJSON.ELEMENT_PRECISION, 0);
				object.put(IDFConstants.IJSON.ELEMENT_SCALE, 0);
				object.put(IDFConstants.IJSON.ELEMENT_POSITION, iColIdx);
				object.put(IDFConstants.IJSON.ELEMENT_DEFAULT, "NONE");
				array.add(object);
			isFieldPopulated=Boolean.TRUE;	
			}
			
			if(isFieldPopulated){
				break;
			}
			
			List<Field> schemaFields=schema.getFields();
			for (Field scField : schemaFields) {
				if(scField.name().equalsIgnoreCase(columnName.replaceAll("\\W", "_"))){
					object=new JSONObject();
					object.put(IDFConstants.IJSON.ELEMENT_NAME, scField.name());
					object.put(IDFConstants.IJSON.ELEMENT_TYPE, scField.schema().getType().toString().toLowerCase());
					object.put(IDFConstants.IJSON.ELEMENT_PRECISION, scField.getProp(IDFConstants.IJSON.ELEMENT_PRECISION));
					object.put(IDFConstants.IJSON.ELEMENT_SCALE, scField.getProp(IDFConstants.IJSON.ELEMENT_SCALE));
					object.put(IDFConstants.IJSON.ELEMENT_POSITION, new Integer(iColIdx));
					object.put(IDFConstants.IJSON.ELEMENT_DEFAULT, scField.schema().getType().toString().equalsIgnoreCase("string")?"NONE":0);
					array.add(object);
					isFieldPopulated=Boolean.TRUE;
					break;
				}
			}
		}
	}
	final String schemaDef= "{ \"namespace\": \"com.optum.df.avro\", \"type\": \"record\", \"name\": \""
			+ levelName.toUpperCase() + "\", \"fields\":"+array.toJSONString()+"}";
	System.out.println(schemaDef);
	Schema mergeSchema=new Schema.Parser().parse(schemaDef);
//	mergeSchema.setFields(fields);
//	System.out.println("mergeSchema:"+mergeSchema);
	return mergeSchema;
}

public String getContentFromHDFSURL(final String hdfsPathURL, final boolean requiredHeader){
	StringBuffer sb=new StringBuffer();
	 try{
         Path pt=new Path(hdfsPathURL);
         FileSystem fs = FileSystem.get(this.conf);
         BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
         String line;
         if(requiredHeader)
         {
        	 line=br.readLine();
        	 sb.append(line);
         }else{
         while ((line=br.readLine()) != null){
        	 sb.append(line);
        	 
         }
         }
 }catch(Exception e){
	 new DFException(e);
 }
	return sb.toString();
}

public String getLevelName(final String fileName){
	String tempFileName=fileName;
	if(fileName.indexOf("/")!=-1)
	{
		tempFileName=tempFileName.substring(fileName.lastIndexOf("/")+1);
	}
	//LTE--LEVELNAME--
	String[] fileTokens=StringUtils.splitByWholeSeparatorPreserveAllTokens(tempFileName, IDFConstants.FILENAME_FIELDS_SEPERATOR);
	String levelName=fileTokens[1];//2 string is level Name
	return levelName;
}
	

public Map<String, List<Schema>> getLocalFilesMetaData(final String iLocalMetaFiles)
{
	System.out.println("iHDFSLocationForMetaFiles"+iLocalMetaFiles);
	Map<String, List<String>> hdfsFileRecursively=readLocalFiles(iLocalMetaFiles);
	System.out.println("Got Meta Files:"+hdfsFileRecursively);
	Map<String, List<Schema>> levelMetaFiles=new HashMap<String, List<Schema>>();
	for (Map.Entry<String, List<String>> metaFiles : hdfsFileRecursively.entrySet()) {
		String levelName=metaFiles.getKey();
		System.out.println("level Name:"+levelName);
		List<String> metaFileLocation=metaFiles.getValue();
		List<Schema> schemas=null;
		if(null!=metaFileLocation && !metaFileLocation.isEmpty()){
			schemas=new ArrayList<Schema>();
			Schema tempSchema=null;
			for (String metaLocation : metaFileLocation) {
				tempSchema=convertMetaToAvroSchemaFile(metaLocation, Boolean.FALSE);
				schemas.add(tempSchema);
			}
			
//			System.out.println(levelName.toUpperCase()+"::::"+schemas);
			levelMetaFiles.put(levelName.toUpperCase(), schemas);
		}
	}
	return levelMetaFiles;
}

public Map<String, List<String>> readLocalFiles(final String iLocalDir){

	System.out.println("----readLocalFiles(final String iLocalDir)-----");
	Map<String, List<String>> completeHDFSfiles=new HashMap<String, List<String>>();
	File dir = new File(iLocalDir);
	if(dir!=null && dir.exists() && dir.isDirectory())
	{
		String[] innerDirs=dir.list();
		for (String innerDir : innerDirs) {
			System.out.println(innerDir);
			List<String> innerFiles=getFilesWithInDir(iLocalDir+"/"+innerDir);
			System.out.println(innerFiles);
			completeHDFSfiles.put(innerDir, innerFiles);
		}
	}
	return completeHDFSfiles;
}

public List<String> getFilesWithInDir(final String dir)
{
	File f=new File(dir);
	List<String> files=null;
	if(null!=dir && f.isDirectory())
	{
		files=new ArrayList<String>();
		String[] listFiles=f.list();
		for (String fileName : listFiles) {
			files.add(dir+"/"+fileName);
		}
	}
	return files;
}

public boolean writeAvroSchemaFiles(Schema oSchema, final String avroSchemaLocation)
{
	FSDataOutputStream out =null;
	try {
		FileSystem fs = FileSystem.get(this.conf);
		final String tableName=oSchema.getName();
		Path outFile = new Path(avroSchemaLocation+"/"+tableName+".avsc");
		if (fs.exists(outFile))
		{
			  System.out.println("Output already exists");
			  return Boolean.FALSE;
		}
		
		out= fs.create(outFile);
		out.writeChars(oSchema.toString());
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}finally{
		if(out!=null)
		{
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			out=null;
		}
	}
	
	return Boolean.TRUE;
}

public Map<String, String> readAvroSchemaFiles(final String avrsoSchemaLocation)
{
	
	Path rootDirectory=new Path(avrsoSchemaLocation); //Path representation of HDFS files
	FileSystem rootFileSystem;
	Map<String, String> completeAvroSchemaFiles=new HashMap<String, String>();
	try 
	{
		rootFileSystem = FileSystem.get(this.conf); //getting HDFS filesystem data
		FileStatus[] status=rootFileSystem.listStatus(rootDirectory); //Listing the files in the filesystem for a given path
		for (FileStatus fileStatus : status) //Iterating to get the directories 
		{ 
		final String name=	fileStatus.getPath().getName();
		String content=getContentFromHDFSURL(fileStatus.getPath().toString(), Boolean.FALSE);
		completeAvroSchemaFiles.put(name, content);
		}
		
	}catch(Exception e)
	{
		e.printStackTrace();
	}
	
	return completeAvroSchemaFiles;
}
public static void main(String[] args) {
	HDFSUtil oHDFSUtil=HDFSUtil.getHDFSUtil(new Configuration());
	Map<String, List<Schema>> files=oHDFSUtil.getLocalFilesMetaData("/home/cloudera/SAIWS/OTSIWS/05222014/poclatestcode/Schema");
	oHDFSUtil.getSchemaForMetaFile("hdfs:/user/cloudera/CNUM_FILES/Q1/LTE--cNum--T201405110900--W051114150--SJSLSMR01LEMS1--sam--2.5.csv", files);
	
}

}

