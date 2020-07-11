package sparksql;

/*
 * Run this in Scala IDE.. (Normal Eclipse seems to give trouble)



 * 
 * Add these Dependencies in Maven pom file
 *  <dependencies>
  <!-- https://mvnrepository.com/artifact/log4j/log4j -->
<dependency>
    <groupId>log4j</groupId>
    <artifactId>log4j</artifactId>
    <version>1.2.17</version>
</dependency>
  
  
  
  <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>2.4.0</version>
</dependency>
  
 */


/*User Ratings.
 * UserID,ItemID,Rating,TimeStamp
196,242,3,881250949

Movie Master Item
ItemID,MovieName,releasedate,empty,IMDB_URL,Genre_unknown,Genre_action,Genre_adventure,Genre_Animation,Genre_childrens,Genre_comedy,Genre_crime,Genre_documentary,Genre_drama,Genre_fantasy,Genre_FilmNoir,Genre_Horror,Genre_Musical,Genre_Mystery,Genre_Romance,Genre_SciFi,Genre_Thriller,Genre_War:int,Genre_Western
1,Toy Story (1995),01-Jan-1995,,http://us.imdb.com/M/title-exact?Toy%20Story%20(1995),0,0,0,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0




 */
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MovieAverageRatingTop5 {
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		// For SparkSQL creating a SparkSession object
		
		SparkSession spark_session = SparkSession.builder().appName("Top20Movies").master("local[*]").getOrCreate();
		//using spark session here, and not the Spark Context. 
		
		// Dataframereader object is required to read data from an external file
		
        DataFrameReader dataFrameReader_Instance = spark_session.read();
        
        //.csv method is used to read data from a csv file.
        
        Dataset<Row> ratings_dataSetObject_userRatingsTransactions = dataFrameReader_Instance.option("header","true").csv("D:\\temp\\in\\userRatingsTransactions.data");
       //data frame and data sets are now merged in new verison as DataSet Row.
        
        System.out.println("* * * *  Print out schema ratings :  ratings_dataSetObject ===");
        ratings_dataSetObject_userRatingsTransactions.printSchema();
        
        // Select the columns ItemID and Rating from the dataset. Also, cast the type of Rating to double.
        
        Dataset<Row> ratings_casted_variables_ItemsRatings = ratings_dataSetObject_userRatingsTransactions.select(ratings_dataSetObject_userRatingsTransactions.col("ItemID"), ratings_dataSetObject_userRatingsTransactions.col("Rating").cast("double"));
       // by default all columns are read as Strings. so we are casting Ratings as Double forceably, as we need average for that.
       //Selecting and Casting the Variables. Choosing the relevant columns for procesing. 
        
        
        
        
        System.out.println("* * * * *  Print out schema ratings_casted_variables_ItemsRatings ===");
        ratings_casted_variables_ItemsRatings.printSchema();
        
        
        
        // Groupby on the column ItemID and finding average on the Column Rating
        // Grouby does the Average. Takes Column based on which it needs to perform the Groupby, and the column of which the Groupby/average is needed 
        
        
        Dataset<Row> ratingsAvg_of_EachItemID = ratings_casted_variables_ItemsRatings.groupBy(ratings_casted_variables_ItemsRatings.col("ItemID")).avg("Rating");
        System.out.println("* * * * *  Print out schema ratingsAvg . Each Items Average Rating. ===");
        ratingsAvg_of_EachItemID.printSchema();
        
        // Renaming the column avg(Rating) using the "as" method to Avg_Rating
       
        Dataset<Row> ratingsAvgRename = ratingsAvg_of_EachItemID.select(ratingsAvg_of_EachItemID.col("ItemID"), ratingsAvg_of_EachItemID.col("avg(Rating)").as("Average_Rating_From_UserRatingDataSet"));
        ratingsAvgRename.printSchema();
        //by default average column will be named avg(rating)
        //rename will use select method, choose the itemID as is, and rename avg(rating) as avergae_rating
        
        
        // Sorting the records based on Avg_Rating in descending order and fetching top 20 records using limit
        //orderby method takes the column on which to order and the asc/desc order for sorting.
        Dataset<Row> userRatingsAvgSorted_Top20 = ratingsAvgRename.orderBy(ratingsAvgRename.col("Average_Rating_From_UserRatingDataSet").desc()).limit(20);
        userRatingsAvgSorted_Top20.show(); //shows / display the actual records.  
        
        
        
        
        // Reading the u.item dataset		-> movieRatingsMaster.item
        // to find the metadata for these Item ID's. 
        Dataset<Row> movieData_MedatataMaster = dataFrameReader_Instance.option("header","true").csv("D:\\temp\\in\\moviesMetadataMaster.item");
        System.out.println(" * * * Print out schema ratings ===");
        movieData_MedatataMaster.printSchema();
        
        
        
        
        // Fetching the columns ItemID and MovieName from dataset. use select method to isolate specific columns. 
        
        Dataset<Row> movieData_ItemID_and_MovieName = movieData_MedatataMaster.select(movieData_MedatataMaster.col("ItemID"), movieData_MedatataMaster.col("MovieName"));
        System.out.println("* * * * Print out schema movieDataLean ===");
        movieData_ItemID_and_MovieName.printSchema();
        
        
        
        
        
        // Joining ratingsAvgSorted and movieData_ItemID_and_MovieName on ItemID column. For Top 20 Observations of UserRatingDataSet
        //ITEMID, MovieName, AverageRating.. * * * * Order might get shuffled due to this Join...
        Dataset<Row> joinedData = userRatingsAvgSorted_Top20.join(movieData_ItemID_and_MovieName, userRatingsAvgSorted_Top20.col("ItemID").equalTo(movieData_ItemID_and_MovieName.col("ItemID")), "inner");
        joinedData.printSchema();
        joinedData.show();
        
        
        
        
        // Fetching the movie name and Avg_Rating.. This is another and better method than above syntax. 
        // Ordering by Rating, and Dropping some columns not required... Need only the specific columns here. = Movie Name and AverageUserRating.
        Dataset<Row> finalData = joinedData.select(joinedData.col("MovieName"), joinedData.col("Average_Rating_From_UserRatingDataSet")).orderBy(joinedData.col("Average_Rating_From_UserRatingDataSet").desc());
        
        // Displaying 
        finalData.show();
	}

}

