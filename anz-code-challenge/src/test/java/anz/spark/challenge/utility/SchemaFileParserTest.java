package anz.spark.challenge.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import anz.spark.challenge.dto.SchemaColumnDto;
import anz.spark.challenge.dto.SchemaDto;

public class SchemaFileParserTest
{
	
	
	@Test
	public void readSchemaFile_Success() throws IOException 
	{
		
		// ARRANGE
		SchemaDto  expectedSchemaDto = new SchemaDto();		
		expectedSchemaDto.columns = new ArrayList<SchemaColumnDto>();
		expectedSchemaDto.primary_keys = new ArrayList<String>();
		
		
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", true, "");
		
		expectedSchemaDto.columns.add(firstColumn);		
		expectedSchemaDto.columns.add(secondColumn);	
		
		expectedSchemaDto.primary_keys.add("State/Territory");
		
		// ACT
		// need to be modified the hardcoded path with mocking
		SchemaDto  actualSchemaDto = SchemaFileParser.readSchemaFile("D:\\Learning\\testlocation\\aus-capitals.json");
		
		// ASSERT		
		assertEquals(actualSchemaDto.primary_keys.size(),  expectedSchemaDto.primary_keys.size());	
		assertEquals(String.join(",", actualSchemaDto.primary_keys), String.join(",", expectedSchemaDto.primary_keys));
		
		
	}
	
	@Test
	public void readSchemaFile_Failure() throws IOException 
	{
		
		// ARRANGE
		SchemaDto  expectedSchemaDto = new SchemaDto();		
		expectedSchemaDto.columns = new ArrayList<SchemaColumnDto>();
		expectedSchemaDto.primary_keys = new ArrayList<String>();
		
		
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", true, "");
		
		expectedSchemaDto.columns.add(firstColumn);		
		expectedSchemaDto.columns.add(secondColumn);	
		
		expectedSchemaDto.primary_keys.add("State/Territory");
		expectedSchemaDto.primary_keys.add("Capital");
		// ACT
		// need to be modified the hardcoded path with mocking
		SchemaDto  actualSchemaDto = SchemaFileParser.readSchemaFile("D:\\Learning\\testlocation\\aus-capitals.json");
		
		// ASSERT		
		assertNotEquals(actualSchemaDto.primary_keys.size(),  expectedSchemaDto.primary_keys.size());	
		assertNotEquals(String.join(",", actualSchemaDto.primary_keys), String.join(",", expectedSchemaDto.primary_keys));
		
		
	}
}