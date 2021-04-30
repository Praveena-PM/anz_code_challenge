package anz.spark.challenge.filevalidator;

/**
 * PrimaryKeyValidator class
 *
 */
public class PrimaryKeyValidator
{
	/**
	    * validatePrimaryKeyCount 
	    *
	    * @param  long, long 
	    * @return int  
	*/
	public static int validatePrimaryKeyCount(long pk_row_count, long transmittedRecordCount)
	{
	   //System.out.println("Primary Key count is :"+pk_row_count);
	   //System.out.println("Transmitted file count is :"+transmittedRecordCount);
	   if  (pk_row_count==transmittedRecordCount)
	   {
		   System.out.println("Primary Key count is matching with Transmitted file count");
		   System.out.println("Primary Key Count Validation succesful");
		   return 0;
		}
	   else
	   {
		   System.out.println("Primary Key count is Not matching with Transmitted file count - Invalid file");
		   System.out.println("Primary Key Count Validation Failed");
		   return 3;
	   }
	}
}