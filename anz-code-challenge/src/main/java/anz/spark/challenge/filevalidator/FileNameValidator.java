package anz.spark.challenge.filevalidator;

/**
 * FieldValidator class
 *
 */
public class FileNameValidator 
{
	/**
	    * validateFileName 
	    *
	    * @param  String, String
	    * @return int  
	*/
	public static int validateFileName(String data_file_name ,String transmittedFileName)
	{	
		//System.out.println("Input file name is :"+data_file_name);
		//System.out.println("Transmitted file name is :"+transmittedFileName);
		if  (data_file_name.equals(transmittedFileName))
		{
		   System.out.println("Input file name is matching with Transmitted file name");
		   System.out.println("File Name Validation succesful");
		   return 0;
		}
		else
		{
		   System.out.println("Input file name is not matching with Transmitted file name - Invalid file");
		   System.out.println("File Name Validation  Failed");
		   return 2;
		}
	}
}
