package com.otsi;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.otsi.constants.IDFConstants;

/**
 * 
 * @author Sainagaraju Vaduka
 * Place holder for InputData 
 *
 */
public class TableMetaData {
	
	//Logging
	private static Log logger=LogFactory.getLog(TableMetaData.class);
	
	//Fields
	private String hdfsRootURL;  //Holds Root Directory location
	private String parentPathURL; //Holds each Table Directory location
	private List<String> childPathURL; // Holds all dat files for a given parentPathURL
	private String metaPathURL; // Holds meta file path
	private String cntrlPathURL;
	private String datFilePathURL;
	private Schema schema;
	private String tableName;
	private Map<String, String> cntrlFileMap;
	private String datFileDelimiter;
	private boolean isFixedWidthFile;
	private Schema managedSchema;// This is managed schema
	private String managedSchemaTable;
	private String hdfsDestinationLocation;
	private String parentDir; // This is actually using for current version (0.11) kite 
	
	//Setters and Getters

	
	public String getHdfsRootURL() {
		return hdfsRootURL;
	}
	public void setHdfsRootURL(String hdfsRootURL) {
		this.hdfsRootURL = hdfsRootURL;
	}
	public String getParentPathURL() {
		return parentPathURL;
	}
	public void setParentPathURL(String parentPathURL) {
		this.parentPathURL = parentPathURL;
	}
	public List<String> getChildPathURL() {
		return childPathURL;
	}
	public void setChildPathURL(List<String> childPathURL) {
		this.childPathURL = childPathURL;
		System.out.println("childPathURL:"+childPathURL);
		init();//This will called once we set the files
	}
	public void init() {
		    System.out.println("Entered method preProcessControlFile");
		    //setting the datafiles
			String datPathURL=getPathURLWithExt(IDFConstants.IFile.EXT_CSV_DAT_FILE);
			setDatFilePathURL(datPathURL);
			
			//setting the cntrl files
//			final String cntrlPathURL=getPathURLWithExt(IDFConstants.IFile.EXT_CTL_FILE);
//			setCntrlPathURL(cntrlPathURL);
//			
//			//setting meta files
//			final String metaPathURL=getPathURLWithExt(IDFConstants.IFile.EXT_META_FILE);
//			setMetaPathURL(metaPathURL);
			
			//setting Parent Dir.
			if(null!=datFilePathURL)
			{
				int iLast=datFilePathURL.lastIndexOf("/");
				int iFirst=datFilePathURL.substring(0, iLast-1).lastIndexOf("/");
				setParentDir(datFilePathURL.substring(iFirst+1, iLast));
			}
			
			
	}
	public String getMetaPathURL() {
		if(null==metaPathURL)
		{
			final String metaPathURL=getPathURLWithExt(IDFConstants.IFile.EXT_META_FILE);
			setMetaPathURL(metaPathURL);
		}
		return this.metaPathURL;
	}
	public void setMetaPathURL(String metaPathURL) {
		this.metaPathURL = metaPathURL;
	}
	public Schema getSchema() {
		return schema;
	}
	public void setSchema(Schema schema) {
		this.schema = schema;
		setTableName(schema.getName());
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getCntrlPathURL() {
		if(null==cntrlPathURL)
		{
			final String cntrlPathURL=getPathURLWithExt(IDFConstants.IFile.EXT_CTL_FILE);
			setCntrlPathURL(cntrlPathURL);
		}
		return this.cntrlPathURL;
	}
	public void setCntrlPathURL(String cntrlPathURL) {
		this.cntrlPathURL = cntrlPathURL;
	}
	
	public String getPathURLWithExt(final String lookEXTType){

		String lookingPathURL=null;
		List<String> childURLS=getChildPathURL();
		
		if(null!=childURLS && !childURLS.isEmpty())
		{
			for (String pathURL : childURLS) 
			{
				final String ext=pathURL.substring(pathURL.lastIndexOf(IDFConstants.IFile.FILENAME_TYPE_SEPARATOR));
				
				if(ext.equalsIgnoreCase(lookEXTType))
				{
					lookingPathURL=pathURL;
					break;
				}
			}
		}
		return lookingPathURL;
	
	}
	public Map<String, String> getCntrlFileMap() {
		return cntrlFileMap;
	}
	public void setCntrlFileMap(Map<String, String> cntrlFileMap) {
		this.cntrlFileMap = cntrlFileMap;
		if(null!=cntrlFileMap && !cntrlFileMap.isEmpty()){
			String delimiter=cntrlFileMap.get(IDFConstants.IFile.KEY_PROP_DELIMITER);
			setDatFileDelimiter(delimiter);
		}
	}
	public String getDatFileDelimiter() {
		return datFileDelimiter;
	}
	public void setDatFileDelimiter(String datFileDelimiter) {
		this.datFileDelimiter = datFileDelimiter;
		if(null==datFileDelimiter)
		{
			setFixedWidthFile(Boolean.TRUE);
		}
	}
	public boolean isFixedWidthFile() {
		logger.info(" Is files for table :"+getTableName()+ " are fixed width files ? "+isFixedWidthFile);
		return isFixedWidthFile;
	}
	public void setFixedWidthFile(boolean isFixedWidthFile) {
		this.isFixedWidthFile = isFixedWidthFile;
	}
	public Schema getManagedSchema() {
		return managedSchema;
	}
	public void setManagedSchema(Schema managedSchema) {
		this.managedSchema = managedSchema;
		if(null!=managedSchema){
			setManagedSchemaTable(managedSchema.getName());
		}
	}
	public String getManagedSchemaTable() {
		return managedSchemaTable;
	}
	//This is called from setManagedSchema
	public void setManagedSchemaTable(String managedSchemaTable) {
		this.managedSchemaTable = managedSchemaTable;
	}
	public String getDatFilePathURL() {
		return datFilePathURL;
	}
	public void setDatFilePathURL(String datFilePathURL) {
		this.datFilePathURL = datFilePathURL;
		System.out.println("datFilePathURL:"+datFilePathURL);
	}
	public String getHdfsDestinationLocation() {
		return hdfsDestinationLocation;
	}
	public void setHdfsDestinationLocation(String hdfsDestinationLocation) {
		this.hdfsDestinationLocation = hdfsDestinationLocation;
	}
	public String getParentDir() {
		if(null==this.parentDir)
		{
			getDatFilePathURL();
		}
		return parentDir;
	}
	public void setParentDir(String parentDir) {
		this.parentDir = parentDir;
	}
	@Override
	public String toString() {
		return "TableMetaData [hdfsRootURL=" + hdfsRootURL + ", parentPathURL="
				+ parentPathURL + ", childPathURL=" + childPathURL
				+ ", metaPathURL=" + metaPathURL + ", cntrlPathURL="
				+ cntrlPathURL + ", datFilePathURL=" + datFilePathURL
				+ ", schema=" + schema + ", tableName=" + tableName
				+ ", cntrlFileMap=" + cntrlFileMap + ", datFileDelimiter="
				+ datFileDelimiter + ", isFixedWidthFile=" + isFixedWidthFile
				+ ", managedSchema=" + managedSchema + ", managedSchemaTable="
				+ managedSchemaTable + ", hdfsDestinationLocation="
				+ hdfsDestinationLocation + ", parentDir=" + parentDir + "]";
	}
	
	
}
