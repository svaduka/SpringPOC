package com.otsi.kite.hive;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

import com.otsi.TableMetaData;
import com.otsi.constants.IDFConstants;
import com.otsi.exception.DFException;
import com.otsi.mapper.CreateAvroDatasetMapper;
import com.otsi.util.DFUtil;
import com.otsi.util.HDFSUtil;
import com.otsi.util.KiteUtil;

public class HiveLoadData extends Configured implements Tool {
	
	private static Log logger=LogFactory.getLog(HiveLoadData.class);

	// This is a singleton object if needed we can change it to pool of objects
	//Please change the configuration by calling overrideConfiguration
	private static HDFSUtil oHDFSUtil;
	@Override
	public int run(String[] args) throws Exception {
		final String METHOD_NAME="-----run(String[] args)-----";
		System.out.println(args);
		Configuration conf = super.getConf();
		System.out.println();
		oHDFSUtil=HDFSUtil.getHDFSUtil(conf);
		System.out.println("oHDFSUtil:"+oHDFSUtil);
		
		
		//Get the root directory
		final String iHDFSRootDir=args[0];
		logger.debug("Root Directory:"+iHDFSRootDir);
		System.out.println("Root Directory:"+iHDFSRootDir);
		//Get RepoURL to work on
//		String repoURL=args[1];
//		logger.debug("Repository URL:"+repoURL);
		
		//Get HDFS Repo to put the files on
		String hdfsRepo=args[1];
		logger.debug("Repository URL:"+hdfsRepo);
		System.out.println("Repository URL:"+hdfsRepo);
		//TODO test mean while
		String repoURL="repo:hive:/*path?path="+hdfsRepo;
		hdfsRepo="repo:hdfs:"+hdfsRepo;
		
		Map<String, List<String>> completeFiles=oHDFSUtil.getHDFSFilesRecursively(iHDFSRootDir);
		System.out.println("completeFiles"+completeFiles);
		//Convert into TableMetaData objects
		List<TableMetaData> metaInfos=DFUtil.populateTableMetaData(iHDFSRootDir, completeFiles);
		System.out.println("metaInfos:"+metaInfos);
		//Conver Meta File to AVSC is done in preProcessInputs
		preProcessInputs(metaInfos,IDFConstants.REPO_LOCATION_FOR_META_FILES);
		processFiles(metaInfos, repoURL,hdfsRepo);
		return 0;
	}
	/**
	 * 
	 * @param metaInfos
	 * @param args
	 * This method will process each TableMetaData and creating the hive tables
	 */
	public void processFiles(List<TableMetaData> metaInfos, final String hiveRepoURL, final String hdfsRepoURL) {
		for (TableMetaData tableMetaData : metaInfos) {
			System.out.println("Entered method processFiles");
			Schema managedSchema=DFUtil.getSchemaFromFile(new File(IDFConstants.LOCATION_SCHEMA_FILE_NAME));
			tableMetaData.setManagedSchema(managedSchema);
			processTableMetaData(tableMetaData, hiveRepoURL, hdfsRepoURL);
			System.out.println("Exited method processFiles "+managedSchema);
		}
	}
	/**
	 * 
	 * @param tableMetaData
	 * @param hiveRepoURL
	 * @param hdfsRepoURL : Destination HDFS Repo to load the avro file
	 * 
	 * 
	 * 1) Register the schema
	 */
	public void processTableMetaData(TableMetaData tableMetaData, final String hiveRepoURL, final String hdfsRepoURL) {
		if(null==tableMetaData.getDatFilePathURL()){
			return;
		}
		System.out.println("Entered processTableMetaData Method");
		KiteUtil oKiteUtil=KiteUtil.getKiteUtil();
		oKiteUtil.setHdfsRepo(hdfsRepoURL);
		oKiteUtil.setHiveRepo(hiveRepoURL);
		//Register Schema
		oKiteUtil.registerSchema(tableMetaData);
		oKiteUtil.createDatasetDescriptor(tableMetaData);//Creating the dataset Desciptor
		boolean isTableCreated=oKiteUtil.createTable(tableMetaData.getSchema());
		if(isTableCreated)
		{
			oKiteUtil.updateSchema(tableMetaData);
		}
//		oKiteUtil.convertDataFileToAvroFile(tableMetaData);
		System.out.println("Creating of Dataset Descriptor is done");
		convertDataFileToAvroFiles(tableMetaData, hdfsRepoURL);
		System.out.println("Exited processTableMetaData Method");
		
	}
	
	/**
	 * 
	 * @param oTableMetaData
	 * Start Execution of MapReduce Process
	 */
	public void convertDataFileToAvroFiles(TableMetaData oTableMetaData,final String hdfsRepoURL){
		System.out.println("Entered convertDataFileToAvroFiles method ");
		Configuration conf=super.getConf(); 
		try {
			conf.set(IDFConstants.KEY_IS_FIXED_SCHEMA, oTableMetaData.isFixedWidthFile()?IDFConstants.BOOLEAN_STRING_Y:IDFConstants.BOOLEAN_STRING_N);
			conf.set(IDFConstants.KEY_SCHEMA_NAME, oTableMetaData.getSchema().toString());
			System.out.println("oTableMetaData.getDatFileDelimiter()"+oTableMetaData.getDatFileDelimiter());
			conf.setInt(IDFConstants.KEY_DELIMITER, new String(",").hashCode());
			
			@SuppressWarnings("deprecation")
			Job job=new Job(conf, "ConvertDataFileToAvroFiles:"+oTableMetaData.getTableName());
			job.setJarByClass(HiveLoadData.class);
			
			job.setMapperClass(CreateAvroDatasetMapper.class);
			job.setNumReduceTasks(0);
			job.setMapOutputKeyClass(GenericRecord.class);
			job.setMapOutputValueClass(Void.class);
			System.out.println("MR classes and keys set..... ");
			DatasetKeyOutputFormat.setRepositoryUri(job, new URI(hdfsRepoURL));
			DatasetKeyOutputFormat.setDatasetName(job, oTableMetaData.getTableName());
			job.setOutputFormatClass(DatasetKeyOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(oTableMetaData.getDatFilePathURL()));
//			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(Boolean.TRUE);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Exited convertDataFileToAvroFiles method ");
	
	}
	/**
	 * 
	 * @param metaInfos
	 * @param repoURL
	 * Creation of Avro Files and placing on HDFS at constant location
	 */
	public void convertDataFileToAvroFiles(List<TableMetaData> metaInfos, final String repoURL) {
		// TODO Auto-generated method stub
		
	}
	/**
	 * 
	 * @param metaInfos
	 * Preprocess steps 
	 * 1) Read Control Files
	 * 2) Read Meta Files
	 * 3) convert Meta Files into Schema files
	 */
	public void preProcessInputs(List<TableMetaData> metaInfos, final String iHDFSLocationForMetaFiles){
		System.out.println("Entered method preProcessInputs");
		
		for (TableMetaData tableMetaData : metaInfos) {
			if( null!=tableMetaData.getDatFilePathURL())
			{
			preProcess(tableMetaData,iHDFSLocationForMetaFiles);
			}
		}
		System.out.println("Exited method preProcessInputs");
		
	}

	public void preProcess(TableMetaData tableMetaData,final String iHDFSLocationForMetaFiles) {
		//Processing control File
//		preProcessControlFile(tableMetaData);
		//processing Meta Files
		preProcessMetaFiles(tableMetaData,iHDFSLocationForMetaFiles);
	}

	public void preProcessMetaFiles(TableMetaData tableMetaData, final String iHDFSLocationForMetaFiles) {
		
		System.out.println("Entered method preProcessMetaFiles");
		String hdfsRepoURLForDatFile=tableMetaData.getDatFilePathURL();
		System.out.println("hdfsRepoURLForMetaFile "+hdfsRepoURLForDatFile);
		logger.debug("Got Meta File Location:"+hdfsRepoURLForDatFile);
		
		//Move to one Place
		Map<String, List<Schema>> allMetaFileInformations=oHDFSUtil.getLocalFilesMetaData(iHDFSLocationForMetaFiles);
		Schema oSchema=oHDFSUtil.getSchemaForMetaFile(hdfsRepoURLForDatFile, allMetaFileInformations);
//		Schema oSchema=oHDFSUtil.convertMetaToAvroSchemaFile(hdfsRepoURLForMetaFile);
		tableMetaData.setSchema(oSchema);
//		System.out.println("oSchema preProcessMetaFiles Method "+oSchema);
		System.out.println("Exited method preProcessMetaFiles");
		
	}
	public void preProcessControlFile(TableMetaData tableMetaData) {
		System.out.println("Entered method preProcessControlFile");
		
		String hdfsRepoURLForControlFile=tableMetaData.getCntrlPathURL();
		logger.debug("Got Control File Location:"+hdfsRepoURLForControlFile);
		
		String controlFileData=oHDFSUtil.getContentFromHDFSURL(hdfsRepoURLForControlFile);
		logger.debug("Got Control File Data:"+controlFileData);		
		
		Map<String, String> cntrlFileMap=DFUtil.getControlFileData(controlFileData, IDFConstants.CONTROL_FILE_DELIMITER);
		tableMetaData.setCntrlFileMap(cntrlFileMap);// Sametime we are populating Delimiter of Data file
		System.out.println("Exited method preProcessControlFile");
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Check input arguments.
		if (null == args || args.length == 0 || args.length < 2) {
			System.out.println("Usage: java HiveLoadData mainDir hdfsDestinationURL");
			System.out.println("Exiting the program no sufficient input arguments");
			System.exit(-1);
		}
		Configuration conf = new Configuration(Boolean.TRUE);
		try {
			int i = ToolRunner.run(conf, new HiveLoadData(), args);
			System.exit(i);
		} catch (Exception e) {
			new DFException(e);
			e.printStackTrace();
			System.exit(-1); //TODO Currently I am Exciting the program going forward we will handle the exception
		}

	}

}
