package anz.spark.challenge.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import org.junit.Test;




public class TagFileParserTest
{
	
	@Test
	public void testParseTagFile_Success() throws IOException 
	{
		//The AAA (Arrange, Act, Assert) pattern is a common way of writing unit tests for a method under test
		// Arrange
		String expected = "aus-capitals.csv|8";		
		
		// Act
		// need to be modified the hardcoded path with mocking
		String actual = TagFileParser.readTagFile("D:\\Learning\\testlocation\\aus-capitals.tag");
		
		// Assert		
		assertEquals(expected, actual);	
	}
	
	
	@Test
	public void testParseTagFile_Failure() throws IOException 
	{
		//The AAA (Arrange, Act, Assert) pattern is a common way of writing unit tests for a method under test
		// Arrange
		String expected = "aus-capitals|8";		
		
		// Act
		// need to be modified the hardcoded path with mocking
		String actual = TagFileParser.readTagFile("D:\\Learning\\testlocation\\aus-capitals.tag");
		
		// Assert		
		assertNotEquals(expected, actual);	
	}
	/*
	@Test
	public void testParseTagFile_Failure() 
	{
		// ASSIGN
		TagDto  expecedTagDto = new TagDto();
		expecedTagDto.fileName = "aus-capitals";
		expecedTagDto.rowCount = 0;	
		
		// ACT
		// need to be modified the hardcoded path with mocking
		TagDto atualTagDto = TagFileParser.parseTagFile("D:\\Learning\\testlocation\\aus-capitals.tag");
		// ASSET
		
		assertNotEquals(expecedTagDto.fileName,atualTagDto.fileName);		
		assertNotEquals(expecedTagDto.rowCount,atualTagDto.rowCount);		
	} */
	
	/*
	private Path workingDir;
 
    @Before
    public void init() {
        this.workingDir = Path.of("", "src/test/resources");
    }

    @Test
    public void read() throws IOException {
        Path file = this.workingDir.resolve("test.file");
        String content = Files.readString(file);
        assertThat(content, is("duke"));
    }
	*/
	

}