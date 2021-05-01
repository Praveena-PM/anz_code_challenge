package anz.spark.challenge.filevalidator;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Unit test for RecordCountValidatorTest
 */
public class RecordCountValidatorTest 

{
    /**
     * Create the test case : to validate the record count input vs tag
     *
     */
	 @Test
	public void testrecordCountValidator() 
	{
		assertEquals(0,RecordCountValidator.validateRecordCount(7,7));
		assertEquals(1,RecordCountValidator.validateRecordCount(9,10));
	}
	

	
} 
