package sparksql;
/*
 * findTop 5 users who rate movies. 
 */
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopFiveUsers {
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		// changed the logging level to reduce the errors on console. 
		
		SparkSession session = SparkSession.builder().appName("Top5Users").master("local[*]").getOrCreate();
		
        DataFrameReader dataFrameReader = session.read();
        Dataset<Row> ratings = dataFrameReader.option("header","true").csv("D:\\temp\\in\\userRatingsTransactions.data"); 
        //Dataset<row> ratings = dataFrameReader.option("header","true").csv(args[0]);
        // In case we want to push the inputfilename from command line. 
        
        // UserID, ItemID, Rating, TimeStamp
        
        Dataset<Row> ratings_OnlyUserID = ratings.select(ratings.col("UserID"));
        
        Dataset<Row> groupedData = ratings_OnlyUserID.groupBy("UserID").count();   
        Dataset<Row> sortedData = groupedData.orderBy(groupedData.col("count").desc()); 
        sortedData.show(5);
	}

}
