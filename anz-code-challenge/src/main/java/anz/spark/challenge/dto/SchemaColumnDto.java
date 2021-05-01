package anz.spark.challenge.dto;

/**
 * SchemaColumnDto class
 *
 */
public class SchemaColumnDto
{
	public String name;
	public String type;
	public boolean mandatory;
	public String format;
	
	/**
	 * SchemaColumnDto class constructor
	 *
	 */
	public SchemaColumnDto(String name, String type, boolean mandatory, String format)
	{
		this.name=name;
		this.type=type;
		this.mandatory=mandatory;
		this.format=format;
	}
}