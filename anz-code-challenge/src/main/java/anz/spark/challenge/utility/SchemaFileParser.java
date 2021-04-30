package  anz.spark.challenge.utility;

import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.Gson;

import anz.spark.challenge.dto.SchemaColumnDto;
import anz.spark.challenge.dto.SchemaDto;

/**
 * This is SchemaFileParser
 *
 */
public class SchemaFileParser
{
	/**
	    * reading readSchemaFile
	    *
	    * @param  String : schema_file_path
	    * @return DTO : Schema DTO
	*/
	
	//Reference used : http://tutorials.jenkov.com/java-json/gson.html#:~:text=GSON%20can%20pase%20JSON%20into,fromJson(json%2C%20Car.
	public static SchemaDto readSchemaFile(String schema_file_path)
	{
		SchemaDto schemaDto = null;
		try
		{
			
			schemaDto=new Gson().fromJson(
					new FileReader (new File(schema_file_path )),
					SchemaDto.class					
					);
			
			//VERIFICATION PURPOSE:converting object to String
			String schemaDtoJSON= new Gson().toJson(schemaDto);
			System.out.println("\n\n"+schemaDtoJSON);
		}
		catch (IOException e) 
		{
			System.out.println(e.getMessage());
		}
		
		
		return schemaDto;
	}
	
	/**
	    *  getColumns
	    *
	    * @param  SchemaDto
	    * @return String
	*/
	
	public static String getColumns(SchemaDto  schemaDto)
	{
		List<SchemaColumnDto> schemaColumnDtoList = schemaDto.columns;	
		
		List<String> columnNamesCheckColumnList = new ArrayList<String>();
		
		for(SchemaColumnDto objColumnModel : schemaColumnDtoList)
		{
			columnNamesCheckColumnList.add(objColumnModel.name);
		}
		
		return "["+ String.join(", ", columnNamesCheckColumnList) + "]";
	}
	
}