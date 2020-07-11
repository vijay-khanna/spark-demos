package sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TopFiveUsers {
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkSession session = SparkSession.builder().appName("Top5Users").master("local[*]").getOrCreate();
		
        DataFrameReader dataFrameReader = session.read();
        Dataset<Row> ratings = dataFrameReader.option("header","true").csv("in/u.data"); 
        
        // UserID, ItemID, Rating, TimeStamp
        
        Dataset<Row> ratingsLean = ratings.select(ratings.col("UserID"));
        
        Dataset<Row> groupedData = ratingsLean.groupBy("UserID").count();   
        Dataset<Row> sortedData = groupedData.orderBy(groupedData.col("count").desc()); 
        sortedData.show(5);
	}

}
