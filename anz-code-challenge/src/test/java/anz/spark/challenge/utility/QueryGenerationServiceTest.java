package anz.spark.challenge.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

import anz.spark.challenge.dto.SchemaColumnDto;
import anz.spark.challenge.dto.SchemaDto;




public class QueryGenerationServiceTest
{	
	
	@Test
	public void testGetNullCheckQueryWithTwoMandatoryColumn() 
	{
		// ARRANGE
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", true, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String expectedResult = "(`State/Territory` is null )  or (`Capital` is null ) ";
		// ACT
		String actualResult = QueryGenerationService.getNullCheckQuery(schemaDto);
		// ASSET
		
		assertEquals(expectedResult,actualResult);		
	}
	
	@Test
	public void testGetNullCheckQueryWithOneMandatoryColumn() 
	{
		// ARRANGE
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		//mandatory false
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", false, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String expectedResult = "(`State/Territory` is null ) ";
		// ACT
		String actualResult = QueryGenerationService.getNullCheckQuery(schemaDto);
		// ASSET
		
		assertEquals(expectedResult,actualResult);		
	}
	
	@Test
	public void testGetNullCheckQueryWithNoMandatoryColumn() 
	{
		// ARRANGE
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		//mandatory false for both
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", false, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", false, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String expectedResult = "";
		// ACT
		String actualResult = QueryGenerationService.getNullCheckQuery(schemaDto);
		
		// ASSERT
		
		assertEquals(expectedResult,actualResult);		
	}
	
	
	
	@Test
	public void testGetIntegerCheckQueryWithTwoMandatoryColumn() 
	{
		// ARRANGE
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("City Population", "INTEGER", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Percentage", "INTEGER", true, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String expectedResult = " cast(`City Population` as int ) is null  or  cast(`Percentage` as int ) is null ";
		// ACT
		String actualResult = QueryGenerationService.getIntegerCheckQuery(schemaDto);
		// ASSET
		
		assertEquals(expectedResult,actualResult);		
	}
	
	@Test
	public void testGetIntegerCheckQueryWithOneMandatoryColumn() 
	{
		// ARRANGE
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("Capital", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Percentage", "INTEGER", true, "");
		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String expectedResult = " cast(`Percentage` as int ) is null ";
		// ACT
		String actualResult = QueryGenerationService.getIntegerCheckQuery(schemaDto);
		// ASSERT
		
		assertEquals(expectedResult,actualResult);		
	}
	
	@Test
	public void testGetIntegerCheckQueryWithNoMandatoryColumn() 
	{
		// ARRANGE
		SchemaDto  schemaDto = new SchemaDto();
		schemaDto.columns = new ArrayList<SchemaColumnDto>();
		SchemaColumnDto firstColumn = new SchemaColumnDto("State/Territory", "STRING", true, "");
		SchemaColumnDto secondColumn = new SchemaColumnDto("Capital", "STRING", true, "");

		schemaDto.columns.add(firstColumn);
		schemaDto.columns.add(secondColumn);
		
		String expectedResult = "";
		// ACT
		String actualResult = QueryGenerationService.getIntegerCheckQuery(schemaDto);
		// ASSERT
		
		assertEquals(expectedResult,actualResult);		
	}
	

}