package anz.spark.challenge.filevalidator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit test for RecordCountValidatorTest
 */
public class FileNameValidatorTest 
  
{
    /**
     * Create the test case : to validate the record count input vs tag
     *
     */
	
	@Test
	public void testfileNameValidator_Test() 
	{
		assertEquals(0,FileNameValidator.validateFileName("abc.csv","abc.csv"));
		assertEquals(2,FileNameValidator.validateFileName("abc","ABC"));
		
	}	
} 
