/**
 * 
 */
package edu.sdsc.mmtf.spark.ml;

import java.io.IOException;

import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.biojava.nbio.structure.StructureException;

/**
 * Runs regression on a given dataset.
 * Dataset are read as Parquet file. The dataset must contain
 * a feature vector named "features" and a prediction column.
 * The column name of the prediction column must be specified
 * on the command lines.
 * 
 * Example: DatasetRegressor example.parquet property
 * 
 * @author Peter Rose
 */
public class DatasetRegressor {

	/**
	 * @param args args[0] path to parquet file, args[1] name of the prediction column
	 * @throws IOException 
	 * @throws StructureException 
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.err.println("Usage: " + DatasetRegressor.class.getSimpleName() + " <parquet file> <prediction column name>");
			System.exit(1);
		}

		// name of the prediction column
		String label = args[1];
		
		long start = System.nanoTime();

		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(DatasetRegressor.class.getSimpleName())
				.getOrCreate();

		Dataset<Row> data = spark.read().parquet(args[0]).cache();
		
		int featureCount = ((DenseVector)data.first().getAs("features")).numActives();
		System.out.println("Feature count: "  + featureCount);

		System.out.println("Dataset size : " + data.count());

		double testFraction = 0.3;
		long seed = 123;

		LinearRegression lr = new LinearRegression()
				.setLabelCol(label)
				.setFeaturesCol("features");
		
		SparkRegressor reg = new SparkRegressor(lr, label, testFraction, seed);
		System.out.println(reg.fit(data));
		
		GBTRegressor gbt = new GBTRegressor()
				.setLabelCol(label)
				.setFeaturesCol("features");
		
		reg = new SparkRegressor(gbt, label, testFraction, seed);
		System.out.println(reg.fit(data));
		
		GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
				.setLabelCol(label)
				.setFeaturesCol("features")
				  .setFamily("gaussian")
				  .setLink("identity")
				  .setMaxIter(10)
				  .setRegParam(0.3);
		
		reg = new SparkRegressor(glr, label, testFraction, seed);
		System.out.println(reg.fit(data));
		
		
		long end = System.nanoTime();

		System.out.println((end-start)/1E9 + " sec");
	}
}
