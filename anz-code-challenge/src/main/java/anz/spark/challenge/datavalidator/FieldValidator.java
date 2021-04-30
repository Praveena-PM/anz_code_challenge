package anz.spark.challenge.datavalidator;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import anz.spark.challenge.dto.SchemaDto;
import anz.spark.challenge.utility.QueryGenerationService;
import anz.spark.challenge.utility.SparkService;


/**
 * FieldValidator class
 *
 */
public class FieldValidator
{
	/**
	    * validate Fields
	    *
	    * @param  SparkSession, SchemaDto, String 
	    * @return Dataset<Row>   
	*/
		public static Dataset<Row> validateField (SparkSession spark, SchemaDto  schemaDto, String inputDsView)
		{
			System.out.println("********FieldValidator - validateField  Start*****");
		 	
			String data_validation_Sql = QueryGenerationService.getDataValidationQuery(schemaDto, inputDsView);
		    System.out.println(" Final Data Integrity Check Query is :" + data_validation_Sql);
		    
		    Dataset<Row> outputDataSet = SparkService.updateDirtyFlag(spark, data_validation_Sql);	
			outputDataSet.show(false);
			
			System.out.println("********FieldValidator - validateField  end*****");
		    
			return outputDataSet;
		}
	
	
}