package com.otsi.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.otsi.TableMetaData;
import com.otsi.constants.IDFConstants;

/**
 * 
 * @author Sainagaraju Vaduka
 * @Date: 05/07/2014
 * Utility to for conversions
 *
 */
public class DFUtil {
	
	private static Log logger=LogFactory.getLog(DFUtil.class);
	
	public static List<TableMetaData> populateTableMetaData(Map<String, List<String>> oHDFSDirAndFiles)
	{
		List<TableMetaData> metaInfo=null;
		if(null!=oHDFSDirAndFiles && !oHDFSDirAndFiles.isEmpty())
		{
			metaInfo=new ArrayList<TableMetaData>();
			TableMetaData metaData=null;
			for(Map.Entry<String, List<String>> info : oHDFSDirAndFiles.entrySet())
			{
				metaData=new TableMetaData();
				metaData.setParentPathURL(info.getKey());
				metaData.setChildPathURL(info.getValue());
				metaInfo.add(metaData);
			}
		}
		return metaInfo;
	}

	
	public static List<TableMetaData> populateTableMetaData(final String iHDFSRootDir,Map<String, List<String>> oHDFSDirAndFiles)
	{
		List<TableMetaData> metaInfo=null;
		if(null!=oHDFSDirAndFiles && !oHDFSDirAndFiles.isEmpty())
		{
			metaInfo=new ArrayList<TableMetaData>();
			TableMetaData metaData=null;
			for(Map.Entry<String, List<String>> info : oHDFSDirAndFiles.entrySet())
			{
				List<String> files=info.getValue();
				for (String hdfsCSVFileLocation : files) {
				metaData=new TableMetaData();
				metaData.setHdfsRootURL(iHDFSRootDir);
				metaData.setParentPathURL(info.getKey());
//				metaData.setChildPathURL(info.getValue());
				metaData.setDatFilePathURL(hdfsCSVFileLocation);
				metaInfo.add(metaData);
				}
			}
		}
		return metaInfo;
	}


	public static Map<String, String> getControlFileData(String controlFileData, final String delimiter) {

		      Map<String, String> cntlFileMap=new HashMap<String, String>();
			  logger.debug("controlFileData is" + controlFileData);
			  if(controlFileData!=null)
			  {
               String[] cntlFileParams = StringUtils.splitByWholeSeparatorPreserveAllTokens(controlFileData, delimiter);
               cntlFileMap.put(IDFConstants.IFile.CNTRL_FILE_PROP_SOURCE_NAME, cntlFileParams[0]);
               cntlFileMap.put(IDFConstants.IFile.CNTRL_FILE_PROP_SCHEMA_NAME, cntlFileParams[1]);
               cntlFileMap.put(IDFConstants.IFile.CNTRL_FILE_PROP_TABLE_NAME, cntlFileParams[2]);
               cntlFileMap.put(IDFConstants.IFile.CNTRL_FILE_PROP_RECORD_COUNT_NAME, cntlFileParams[3]);
             //Getting the delimiter as per kannam email with subject: Delimiter Change - New Format dated : 01/27/2014
               cntlFileMap.put(IDFConstants.IFile.KEY_PROP_DELIMITER, getDelimeterFromCntrlFile(controlFileData,delimiter)); 
               cntlFileMap.put(IDFConstants.IFile.CNTRL_FILE_PROP_EXT_TS, cntlFileParams[7]);
			  }
               logger.info("rec count is "+cntlFileMap.get(IDFConstants.IFile.CNTRL_FILE_PROP_RECORD_COUNT_NAME));
               
		return cntlFileMap;
	}
	
	 public static String getDelimeterFromCntrlFile(final String iCntrlLine, final String delimeter){
			String delimiterInControlFile=null;
			final String METHODNAME="-----getDelimeterFromCntrlFile(final String iCntrlLine)-----";
			logger.info("ENTRY METHOD:"+METHODNAME); //logging
			logger.info("CONTROL FILE INPUT:"+iCntrlLine);//logging
			logger.info("Got Delimiter from properties file:"+delimeter);//logging
			if(!StringUtils.isEmpty(iCntrlLine)&& !StringUtils.isEmpty(delimeter) && StringUtils.contains(iCntrlLine,delimeter)){
				String[] splits=StringUtils.splitByWholeSeparatorPreserveAllTokens(iCntrlLine, delimeter);
				//Interest in second value.d
				for(int j=0;j<splits.length;j++){
				final String secondSplit=splits[j];
				if(secondSplit.indexOf("delimited")==-1){
					continue;
				}
				int iStartIdx=iCntrlLine.indexOf("'");
				int iEndIdx=iCntrlLine.indexOf("'",iStartIdx+1);
				delimiterInControlFile=StringUtils.substring(iCntrlLine, iStartIdx+1, iEndIdx);
				}
				delimiterInControlFile=delimiterInControlFile==null?IDFConstants.IFile.CONTROL_FILE_FIXED_WIDTH_FILE_INDICATOR:delimiterInControlFile;
			}
			logger.info("GOT DELIMITER FROM CONTROL FILE:"+delimiterInControlFile);
			logger.info("EXIT METHOD:"+METHODNAME);
			return delimiterInControlFile;
		}

	 public static Schema getSchemaFromFile(final File avscFile){
			Schema oSchema=null;
			try {
				oSchema = new Schema.Parser().parse(avscFile);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return oSchema;
		}


	public static boolean compareLists(List<Field> compareList,List<Field> comparedList) {
		if (compareList.size() == comparedList.size()) 
		{
			int iLenght=compareList.size();
			Field compareField=null;
			Field comparedField=null;
			for(int i =0; i<iLenght;i++)
			{
				compareField=compareList.get(i);
				comparedField=comparedList.get(i);
				if(compareField.pos()!=comparedField.pos() || !compareField.name().equalsIgnoreCase(comparedField.name()))
				{
					return Boolean.FALSE;
				}
		} 
		}else{
			return Boolean.FALSE;
		}
		return Boolean.TRUE;
	}
}
