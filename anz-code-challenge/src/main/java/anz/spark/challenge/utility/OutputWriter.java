package anz.spark.challenge.utility;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


/**
 * OutputWriter class
 *
 */
public class OutputWriter
{
	/**
	    * writeOutputFile Fields
	    *
	    * @param  Dataset<Row>, String 
	    * @return void  
	*/
	
	public static void writeOutputFile(Dataset<Row> outputDs,String output_file_path)
	{
		System.out.println("********OutputWriter - writeOutputFile  Start*****");
		File file = new File(output_file_path);
		String parentOutputDirName = file.getAbsoluteFile().getParent();		
		System.out.println("parentDirName is: " + parentOutputDirName);	
		
		String tempOutputDir=parentOutputDirName + "\\" + "tempoutput";
		System.out.println("tempOutputDir is: "+ tempOutputDir);
		
		outputDs.write()
		      .option("header", "true")
		      	.mode("overwrite")
		             .csv(tempOutputDir);
		
		//outputDs.write().csv(tempOutputDir).option("header", "true");
		System.out.println("********OutputWriter - writeOutputFile  End*****");
		
	}
}