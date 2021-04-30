package anz.spark.challenge.filevalidator;

/**
 * RecordCountValidator class
 *
 */
public class RecordCountValidator
{
	/**
	    * validateRecordCount 
	    *
	    * @param  long, long 
	    * @return int  
	*/
	public static int validateRecordCount(long inputDsCount, long transmittedRecordCount)
	{
	   //System.out.println("Input file count is :"+inputDsCount);
	   //System.out.println("Transmitted file count is :"+transmittedRecordCount);
	   if  (inputDsCount==transmittedRecordCount)
	   {
		   System.out.println("Input file count is matching with Transmitted file count");
		   System.out.println("Record Count Validation succesful");
		   return 0;
		}
	   else
	   {
		   System.out.println("Input file count is Not matching with Transmitted file count - Invalid file");
		   System.out.println("Record Count Validation Failed");
		   return 1;
	   }
	   
	}
}