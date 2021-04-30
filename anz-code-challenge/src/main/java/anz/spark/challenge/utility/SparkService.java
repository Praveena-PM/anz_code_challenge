package anz.spark.challenge.utility;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.sql.SparkSession;


/**
 * SparkService class
 *
 */

public class SparkService 
{
	static String APP_NAME="ANZ CODE CHALLENGE";
 	static String CSV_FILE_FORMAT="csv";
 	
	/**
	    * Creating a spark session
	    *
	    * @param  None
	    * @return SparkSession object
	*/
	
	public static SparkSession createSparkSession()
	{
		SparkSession spark= SparkSession
			.builder()
	 		.appName(APP_NAME)
			.getOrCreate(); 
			
		registerUdfDateValidation(spark);
		spark.sparkContext().setLogLevel("ERROR");
		return spark;
		
    }
	
	/**
	    * stopSparkSession
	    *
	    * @param  SparkSession
	    * @return void
	*/
	public static void stopSparkSession (SparkSession spark)
	{
		if (spark != null)
		{
			spark.stop();
		}
	}
	
	/**
	    * readCSVFile
	    *
	    * @param  SparkSession, String
	    * @return Dataset<Row>
	*/
	public static Dataset<Row> readCSVFile(SparkSession spark, String data_file_path)
	{
		return spark
				.read()
				.format(CSV_FILE_FORMAT)
				.option("header", "true")
				.load(data_file_path)
				.cache();
		
	}
	
	private static void registerUdfDateValidation(SparkSession spark)
	{			
		// UDF2 object creation with custom method
		// referred link https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html
		UDF2 validateDateFormat = new UDF2<String, String, String>()
		{
				// First two strings are input type and last one is output type
				// call method gets invoked while executing the sql script (@override method)
			    @Override
		        public String call(String str_date, String str_format) throws Exception
		        {
		        	// creating object with input format
		            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(str_format);		       
		            try
		            {
		            	// just checking the given string is convertable to given format
		            	Date formatedDate = simpleDateFormat.parse(str_date.trim());
		            }    
		            catch (ParseException e)
		            {
		            	return "invalid";
		            }    
		            return "valid";
		        }
		};
		spark.udf().register("validateDateFormat", validateDateFormat, DataTypes.StringType);
	}
	
	
	
	/**
	    * Executing the method createDsView which to create a temp view from the input file dataset
	    *
	    * @param  String input DS Name: inputDs, view name : inputDsView
	    * @return None
	*/

	public static void createDsView(Dataset<Row> inputDs,  String inputDsView )
	{
		inputDs.createOrReplaceTempView(inputDsView);
	}
	
	/**
	    * Executing the method getPrimaryKeyCount which will give unique record count for the primary Key column
	    *
	    * @param  SparkSession object,Query whichs hould get executed
	    * @return long count value
	*/
	public static long getPrimaryKeyCount(SparkSession spark,String primaryKeyCountSql)
	{
		return spark.sql(primaryKeyCountSql).count();
	}
	
	/**
	    * updateDirtyFlag
	    *
	    * @param  SparkSession, String
	    * @return Dataset<Row>
	*/
	
	public static Dataset<Row> updateDirtyFlag(SparkSession spark, String data_validation_Sql)
	{
		return spark.sql(data_validation_Sql);
		
				
	}
}