package anz.spark.challenge.utility;

import java.util.ArrayList;
import java.util.List;

import anz.spark.challenge.dto.SchemaColumnDto;
import anz.spark.challenge.dto.SchemaDto;

/**
 * QueryGenerationService class
 *
 */
public class QueryGenerationService
{
	
	/**
	    * getPrimaryKeyQuery 
	    *
	    * @param  List<String> , String 
	    * @return String  
	*/
	
	public static String getPrimaryKeyQuery( List<String> pkColumnList, String inputDsView)
	{
	  String primaryKeyCountSql="";
	  List<String> pkCountCheckList=new ArrayList<String>();
	  
      for (String primaryKeyCol : pkColumnList)
      {
    	  pkCountCheckList.add("`"+primaryKeyCol+"`");
      }
      
      if (!pkCountCheckList.isEmpty())
      {
    	  primaryKeyCountSql="SELECT DISTINCT "+String.join(",",pkCountCheckList)+" FROM inputDsView"; 
      }
      
       return primaryKeyCountSql;
	}
	
	/**
	    * getNullCheckQuery 
	    *
	    * @param  SchemaDto
	    * @return String  
	*/
	
	public static String getNullCheckQuery(SchemaDto schemaDto)
	{

		List <SchemaColumnDto> schemaColumnDtoList = schemaDto.columns;
		List<String> nullCheckColumnList = new ArrayList<String>();
		
		for (SchemaColumnDto schemaColumnDto:schemaColumnDtoList)
		{
			if (schemaColumnDto.mandatory==true)
			{
				nullCheckColumnList.add("(`" + schemaColumnDto.name + "`" + " is null ) ");
			}
		}
		return String.join(" or ", nullCheckColumnList);
	}
	
	
	/**
	    * getIntegerCheckQuery 
	    *
	    * @param  SchemaDto
	    * @return String  
	*/
	public static String getIntegerCheckQuery(SchemaDto  schemaDto)
	{
		List<SchemaColumnDto> schemaColumnDtoList = schemaDto.columns;	
		
		List<String> integerCheckColumnList = new ArrayList<String>();
		
		for(SchemaColumnDto schemaColumnDto : schemaColumnDtoList)
		{
			if(schemaColumnDto.type.equalsIgnoreCase("INTEGER"))
			{
				integerCheckColumnList.add( " cast(`" + schemaColumnDto.name  + "` as int ) is null "  );
			}
		}
		return String.join(" or ", integerCheckColumnList);
	}
	
	/**
	    * getDateCheckQuery 
	    *
	    * @param  SchemaDto
	    * @return String  
	*/
	public static String getDateCheckQuery(SchemaDto  schemaDto)
	{
		List<SchemaColumnDto> schemaColumnDtoList = schemaDto.columns;	
		
		List<String> dateCheckColumnList = new ArrayList<String>();
		
		for(SchemaColumnDto objColumnModel : schemaColumnDtoList)
		{
			if((objColumnModel.type.equalsIgnoreCase("DATE")) && !(objColumnModel.format.isEmpty()))
			{
				dateCheckColumnList.add( "validateDateFormat(" + 
												objColumnModel.name+ ",'"+
												objColumnModel.format+
												"')== 'invalid'");
				
			}
		}
		
		return String.join(" or ", dateCheckColumnList);
	}
	
	/**
	    * getDataValidationQuery 
	    *
	    * @param  SchemaDto, String
	    * @return String  
	*/
	public static String getDataValidationQuery(SchemaDto  schemaModel, String inputDsView)
	{
		List<String> validationSqlList = new ArrayList<String>();
		
		validationSqlList.add(getNullCheckQuery(schemaModel));
		validationSqlList.add(getIntegerCheckQuery(schemaModel));
		validationSqlList.add( getDateCheckQuery(schemaModel));
	    
	       
	    String validationSqlStr= String.join(" or ", validationSqlList);
	    
	    if (! validationSqlStr.isEmpty())	
	    {
			return  "SELECT *,  CASE "
							+ "WHEN " + validationSqlStr
							+" THEN '1' ELSE '0' END AS dirty_flag " +
					" FROM  " + inputDsView ;
	    }
		else
		{
			return ""; 
		}
	}
	
	
}
	
