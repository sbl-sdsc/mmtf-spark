package edu.sdsc.mmtf.spark.interactions.demos;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.applications.WaterInteractions;

/**
 * Analyzes water interactions with four interacting neighbor atoms.
 * This class use the output file created by {@link WaterInteractions}
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class AnalyzeWaterInteractions4 {

	public static void main(String[] args) throws IOException {

		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(AnalyzeWaterInteractions4.class.getSimpleName())
				.getOrCreate();

		String inputfile = args[0];
        Dataset<Row> data = spark.read().parquet(inputfile).cache();
		
        System.out.println("Interactions: " + data.count());
		
        data.describe("distance1").show();
        data.describe("distance2").show();
        data.describe("distance3").show();
        data.describe("distance4").show();
        
		// all interaction distance statistics
		Dataset<Row> distance = data.select(col("distance1").as("distance"))
				.union(data.select(col("distance2").as("distance")))
				.union(data.select(col("distance3").as("distance")))
				.union(data.select(col("distance4").as("distance")));
		
		distance.describe("distance").show();
		
		// water oxygen - oxygen distance statistics
		Dataset<Row> ooDistance = data.filter("element1 = 'O'").select(col("distance1").as("O-Odistance"))
				.union(data.filter("element2 = 'O'").select(col("distance2").as("O-Odistance")))
				.union(data.filter("element3 = 'O'").select(col("distance3").as("O-Odistance")))
				.union(data.filter("element4 = 'O'").select(col("distance4").as("O-Odistance")));
		
		ooDistance.describe("O-Odistance").show();
		
		// water oxygen - nitrogen distance statistics
		Dataset<Row> onDistance = data.filter("element1 = 'N'").select(col("distance1").as("O-Ndistance"))
				.union(data.filter("element2 = 'N'").select(col("distance2").as("O-Ndistance")))
				.union(data.filter("element3 = 'N'").select(col("distance3").as("O-Ndistance")))
				.union(data.filter("element4 = 'N'").select(col("distance4").as("O-Ndistance")));
		
		onDistance.describe("O-Ndistance").show();
		
		// orientational order statistics
		data.describe("q3").show();
		data.describe("q4").show();


		// angle statistics
		Dataset<Row> allAngles = data.select(col("angle1-2").as("angle"))
				.union(data.select(col("angle1-3").as("angle")))
				.union(data.select(col("angle1-4").as("angle")))
				.union(data.select(col("angle2-3").as("angle")))
				.union(data.select(col("angle2-4").as("angle")))
				.union(data.select(col("angle3-4").as("angle")));
		
		allAngles.describe("angle").show();
		
		// normalized B-factor statistics
		Dataset<Row> nbFactor = data.select(col("nbFactor0").as("nbFactor"))
				.union(data.select(col("nbFactor1").as("nbFactor")))
				.union(data.select(col("nbFactor2").as("nbFactor")))
				.union(data.select(col("nbFactor3").as("nbFactor")))
				.union(data.select(col("nbFactor4").as("nbFactor")));
		
		nbFactor.describe("nbFactor").show();

		spark.close();
	}
}
