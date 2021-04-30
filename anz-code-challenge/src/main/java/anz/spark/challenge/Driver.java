package anz.spark.challenge;

import anz.spark.challenge.utility.*; 
import anz.spark.challenge.datavalidator.*;
import anz.spark.challenge.filevalidator.*;
import anz.spark.challenge.dto.SchemaDto;

import java.io.File;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * This is the entry class - Driver
 *
 */
public class Driver 
{
	/**
	    * Main method to get input file from command line 
	    *
	    * @param  four arguments which are the file path for data, tag, schema and output file
	    * @return void
	*/
    public static void main(String[] args)
    {
	     System.out.println("********ANZ-CODE-CHALLENGE STARTS*********"); 

	     if (args.length != 4)
	     {
	    	 System.out.println(" Four arguments expected, but actual passed is :" + args.length);
	    	 System.out.println("Usage: <data_file_path> <tag_file_path> <schema_file_path> <output_file_path> ");
	    	 System.exit(-1);
	     }
	     
	    String data_file_path 	= args[0];
	    String tag_file_path 	= args[1];
	    String schema_file_path	= args[2];
	    String output_file_path	= args[3];
	     
	     int returnCode = processFiles(data_file_path, tag_file_path, 
	    		 						schema_file_path, output_file_path);
	     System.out.println("******** RETURN CODE *********" + returnCode); 
	     System.out.println("********ANZ-CODE-CHALLENGE ENDS*********");  
	     System.exit(returnCode);        
    }
    
    
    /**
	    * This method process the input files 
	    *
	    * @param  four parameters which are the file path for data, tag, schema and output file
	    * @return int
	*/
    
    public static int processFiles(String data_file_path, String tag_file_path, 
    									String schema_file_path, String output_file_path)
    {   
    	int returnCode = 0;
    	
    	try
    	{
    		System.out.println("********CREATING SparkSession *****");
        	SparkSession spark = SparkService.createSparkSession();   	
        	System.out.println("*******spark CREATED********");
        	
        	System.out.println("********CREATING inputDs from CSV FILE*****");
        	//Read the input csv data file and store into a spark dataSet  
        	Dataset<Row> inputDs = SparkService.readCSVFile(spark, data_file_path);    	
        	System.out.println("*******inputDs CREATED********");
        	
        	System.out.println("********READING TAG FILE START*****");
        	//Parse the tage file and get the  transmitted inputFileName and the record count to validate
        	System.out.println("********READING TAG FILE*****");
        	String tagFileLine = TagFileParser.readTagFile(tag_file_path)  ;     
        	System.out.println("********READING TAG FILE COMPLETED*****");   
			
	    	System.out.println("********SPLITTING TAG FILE LINE START*****" + tagFileLine);
	    	// pipe is used in the tag file to seperate the data items
	    	String[] tagLine = tagFileLine.split("\\|"); 	 
	    	System.out.println( tagLine[0] +" : " + tagLine[1]);	    	
    		String transmittedFileName = tagLine[0];
    		long transmittedRecordCount=Long.parseLong(tagLine[1]);
			System.out.println("********SPLITTING TAG FILE LINE COMPLETED*****");
        
    		//Record Count Validation 
    		returnCode=RecordCountValidator.validateRecordCount(inputDs.count(), transmittedRecordCount);		
    		if (returnCode != 0)
    		{
    			System.out.println("********RECORD COUNT VALIDATION FAILED*****");
    			return returnCode;
    		}
    		
    		//Get the filename from the input file path passed as argument to use in the fileName validation
        	String data_file_name = new File(data_file_path).getName();
        	//File Name Validation 
    		returnCode=FileNameValidator.validateFileName(data_file_name, transmittedFileName);
    		if (returnCode != 0)
    		{
    			System.out.println("********FILE NAME VALIDATION FAILED*****");
    			return returnCode;
    		}
    		
    		//Invoke createDsView to create a temp view named "inputDsView", which will be used later in validation
    		String inputDsViewName = "inputDsView";
    		
    		System.out.println("********CREATE DATASET VIEW STARTED *****");		
    		SparkService.createDsView(inputDs, inputDsViewName);
    		System.out.println("********CREATE DATASET VIEW COMPLETED *****");
    		
    		System.out.println("********READING SCHEMA FILE STARTED *****");
    		//Invoke readSchemaFile to parse the Schema Json file and get the schema details in the form of Schema Dto
    		SchemaDto schemaDto = SchemaFileParser.readSchemaFile(schema_file_path);  
    		System.out.println("********READING SCHEMA FILE COMPLETED *****");
    		
    		System.out.println("********GENERATING PRIMARY KEY COUNT QUERY STARTED *****");
    		//Get the query string to be executed to get the count of primary keys to use later in PK validation
    		String primaryKeyCountSql = QueryGenerationService.getPrimaryKeyQuery(schemaDto.primary_keys, inputDsViewName);		
    		System.out.println("Generated query to get primary Key column count is: "+ primaryKeyCountSql);
    		System.out.println("********GENERATING PRIMARY KEY COUNT QUERY COMPLETED *****");
    		
    		System.out.println("********INVOKE PRIMARY KEY COUNT QUERY EXECUTION METHOD STARTED *****");
    		long pk_row_count=SparkService.getPrimaryKeyCount(spark, primaryKeyCountSql);	
    		System.out.println("********INVOKE PRIMARY KEY COUNT QUERY EXECUTION METHOD COMPLETED *****");
    		// PRIMARY KEY RECORD COUNT Validation 
    		returnCode=PrimaryKeyValidator.validatePrimaryKeyCount(pk_row_count,transmittedRecordCount);
    		if (returnCode != 0)
    		{
    			System.out.println("********PRIMARY KEY RECORD COUNT VALIDATION FAILED*****");
    			return returnCode;
    		}

    		// COLUMN NAME Validation 
    		returnCode=ColumnValidator.validateColumn(Arrays.toString(inputDs.columns()),schemaDto);		
    		if (returnCode != 0)
    		{
    			System.out.println("********COLUMN NAME VALIDATION FAILED*****");
    			return returnCode;
    		}
    		
    		System.out.println("********FIELD VALIDATION STARTED *****");
    		Dataset<Row> outputDs = FieldValidator.validateField(spark, schemaDto, inputDsViewName);
    		System.out.println("********FIELD VALIDATION COMPLETED *****");
    		
    		
    		System.out.println("********OUTPUT WRITER STARTED *****");
    		//Writing the final output to a file
    		OutputWriter.writeOutputFile(outputDs, output_file_path);
    		System.out.println("********OUTPUT WRITER COMPLETED *****");
    		
    	}
    	catch(Exception e)
    	{
    		System.out.println("EXCEPTION OCCURED : " + e.getMessage());	
			return -1;
    	}   	
    	 
    	return returnCode;
    }
    
    
}
