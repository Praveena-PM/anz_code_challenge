package  anz.spark.challenge.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


/**
 * TagFileParser class
 *
 */

public class TagFileParser
{
	/**
	    * reading TagFile
	    *
	    * @param  String : tag_file_path
	    * @return String : Tag file line
	    * @throws IOException 
	*/
	 
	public static String readTagFile(String tag_file_path) throws IOException
	{
		 // We can return pair as well - like this  Pair<String, Long> pair = new Pair<String, Long>("", 0); 
		System.out.println("********TagFileParser - readTagFile  Start*****");
		String line = "";		
		try
		{
			BufferedReader br=new BufferedReader(new FileReader (new File(tag_file_path )));
			line =br.readLine();
			br.close();	
		}
		catch (IOException e) 
		{
			System.out.println("EXCEPTION OCCURED : " + e.getMessage());	
			throw e;
		}
		System.out.println("********TagFileParser - readTagFile  End*****");
		return line;
	}
	
}