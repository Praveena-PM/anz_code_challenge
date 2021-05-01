package anz.spark.challenge.filevalidator;


import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import anz.spark.challenge.dto.SchemaDto;
import anz.spark.challenge.dto.SchemaColumnDto;


/**
 * Unit test for RecordCountValidatorTest
 */
public class ColumnValidatorTest   
{
    /**
     * Create the test case : to validate the record count input vs tag
     *
     */
	 @Test
	public void testColumnValidator_Success() 
	{				
		// ASSIGN
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", true, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);		
		String input_file_column = "[State/Territory, Capital]";
		
		// ACT
		int actualResult = ColumnValidator.validateColumn(input_file_column, schemaDto);
		
		// ASSERT		
		assertEquals(0,actualResult);	
		
	}
	 @Test
	public void testColumnValidator_Failure() 
	{				
		// ASSIGN
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", true, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String input_file_column = "[State/Territory]";
		
		// ACT
		int actualResult = ColumnValidator.validateColumn(input_file_column, schemaDto);
		
		// ASSERT		
		assertEquals(4,actualResult);	
		
	}
	


	
} 
