PROJECT NAME: ANZ-CODE-CHALLENGE
PROJECT VERSION : 0.0.1
JAR FILENAME: anz-code-challenge-0.0.1.jar
PROGRAMMING LANGUAGE : JAVA (JavaSE 11)
PROJECT TYPE: MAVEN PROJECT
IDE USED FOR DEVELOPMENT: Eclipse (Version: 2021-03 (4.19.0))


PROJECT DESCRIPTION:
=======================

Develop a Framework in Java program that  specifically does the below:

1. Validating the file integrity of all feeds
2. Validating and quality checking the data – flagging any rows that do not conform

EXPECTED INPUT:

1. CSV files only
2. Metadata driven – we will provide sample JSON files describing the data
3. File Integrity test – verify we have received all data transmitted from the source
4. Field Integrity test – perform data quality check


ASSUMPTIONS:
1. Data files are provided in a valid CSV format (with a header row)
2. Valid TAG file will be provided
3. Valid SCHEMA (JSON) file provided
4. A NULL value in CSV will contain no whitespace (no requirements to trim)



PROJECT DEPENDENCIES:
====================

1)JUnit :version 4.12
	JUnit is a unit testing framework for Java

2)Spark Project Core : version 3.1.1
	Spark Project Core

3)Spark Project SQL : version 3.1.1
	Spark Project SQL
	
4) Gson : version 2.8.6
	Java library that can be used to convert Java Objects into their JSON representation
	



Scenarios Tested using Spark-Submit Command: All test cases passed succesfully.
=============================================
Note: Expected to change all the paths below according to the testing environment.

Specification 1 : File Integrity Test 

Scenario1:  RETURN CODE of “0”
  .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals.csv D:\Learning\testlocation\aus-capitals.tag  D:\Learning\testlocation\aus-capitals.json  D:\Learning\testlocation\output\sbe-1-1.csv
  
Scenario2:  RETURN CODE of “1”
  .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals.csv D:\Learning\testlocation\aus-capitals-invalid-1.tag D:\Learning\testlocation\aus-capitals.json  D:\Learning\testlocation\output\sbe-1-2.csv
  
Scenario3:  RETURN CODE of “2”
  .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals.csv D:\Learning\testlocation\aus-capitals-invalid-2.tag D:\Learning\testlocation\aus-capitals.json  D:\Learning\testlocation\output\sbe-1-3.csv
  
 Scenario4:  RETURN CODE of “3”
  .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals-dupes.csv   D:\Learning\testlocation\aus-capitals.tag  D:\Learning\testlocation\aus-capitals.json D:\Learning\testlocation\output\sbe-1-4.csv

  *****   tag file name should match with data file name
  ***** 	  as tag file has aus-capitals.csv but input data file is aus-capitals-dupes.csv.
  *****  Hence  file name mismatch is happening and returning a different returncode.
  *****  Created a new tag file and referred in the scenario as given below
    
		  
   .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals-dupes.csv   D:\Learning\testlocation\aus-capitals-dupes.tag  D:\Learning\testlocation\aus-capitals.json D:\Learning\testlocation\output\sbe-1-4.csv

 Scenario5:  RETURN CODE of “4”

   .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals-missing.csv   D:\Learning\testlocation\aus-capitals-missing.tag  D:\Learning\testlocation\aus-capitals.json D:\Learning\testlocation\output\sbe-1-5.csv
  

  Scenario6:  RETURN CODE of “4”
    .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals-addition.csv   D:\Learning\testlocation\aus-capitals-addition.tag  D:\Learning\testlocation\aus-capitals.json D:\Learning\testlocation\output\sbe-1-6.csv



Specification 2 : Data Integrity Test

 Scenario1:  RETURN CODE of “0”	
	  .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals.csv D:\Learning\testlocation\aus-capitals.tag  D:\Learning\testlocation\aus-capitals.json  D:\Learning\testlocation\output\act-sbe2-1.csv
	

 Scenario2:  RETURN CODE of “0”	
  
   .\bin\spark-submit --class anz.spark.challenge.Driver --master local D:\Learning\testlocation\anz-code-challenge-0.0.1.jar  D:\Learning\testlocation\aus-capitals-invalid-3.csv D:\Learning\testlocation\aus-capitals-invalid-3.tag  D:\Learning\testlocation\aus-capitals.json  D:\Learning\testlocation\output\act-sbe2-2.csv




Covered:
=========
• Naming standards used
• Comments
• Performance and scalability
• Project structure and general coding conventions
• Demonstrated knowledge of Git ( commit messages)
• Unit Testing - coverage upto only 30%
• Integration Testing - Completed


Not Covered:
==============
• Dependency injection 
• Command line  utility (to fetch and assign Named arguments)
• Output file with specified name
