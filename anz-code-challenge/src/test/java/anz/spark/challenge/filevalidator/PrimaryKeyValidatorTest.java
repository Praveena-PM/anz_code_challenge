package anz.spark.challenge.filevalidator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


/**
 * Unit test for RecordCountValidatorTest
 */
public class PrimaryKeyValidatorTest
   
{
    /**
     * Create the test case : to validate the record count input vs tag
     *
     */
	 @Test
	public void testValidatePrimaryKeyCount() 
	{
		assertEquals(0,PrimaryKeyValidator.validatePrimaryKeyCount(7,7));
		assertEquals(3,PrimaryKeyValidator.validatePrimaryKeyCount(9,10));
	}

	
} 
