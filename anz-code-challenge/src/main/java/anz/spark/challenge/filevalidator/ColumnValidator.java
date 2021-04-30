package anz.spark.challenge.filevalidator;

import anz.spark.challenge.dto.SchemaDto;
import anz.spark.challenge.utility.SchemaFileParser;


/**
 * ColumnValidator class
 *
 */
public class ColumnValidator 
{
	/**
	    * validateColumn 
	    *
	    * @param  String, SchemaDto 
	    * @return int  
	*/
	public static int validateColumn(String input_file_column, SchemaDto  schemaDto)
	{
		System.out.println("********ColumnValidator - validateColumn  Start*****");
		String schemaColumns = SchemaFileParser.getColumns(schemaDto);
		int result = columnCompare(input_file_column, schemaColumns);
		System.out.println("********ColumnValidator - validateColumn  end*****");
		return result;
	}
		
	
	// Compare the columns from input data file and schema file 	
	private static int columnCompare(String a_data_file_columns, String a_schema_file_columns_name)
	{	
		if (a_data_file_columns.equals(a_schema_file_columns_name))
		{
			System.out.println("Schema vs Data column validation success ");
			return 0;
		}
		else
		{
			System.out.println("Schema vs Data column validation failed");
			return 4;
		}
	}
}