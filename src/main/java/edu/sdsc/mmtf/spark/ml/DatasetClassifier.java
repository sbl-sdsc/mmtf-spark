/**
 * 
 */
package edu.sdsc.mmtf.spark.ml;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.biojava.nbio.structure.StructureException;

/**
 * Runs binary and multi-class classifiers on a given dataset.
 * Dataset are read as Parquet file. The dataset must contain
 * a feature vector named "features" and a classification column.
 * The column name of the classification column must be specified
 * on the command lines.
 * 
 * Example: DatasetClassifier example.parquet label
 * 
 * @author Peter Rose
 *
 */
public class DatasetClassifier {

	/**
	 * @param args args[0] path to parquet file, args[1] name of classification column
	 * @throws IOException 
	 * @throws StructureException 
	 */
	public static void main(String[] args) throws IOException {

		if (args.length != 2) {
			System.err.println("Usage: " + DatasetClassifier.class.getSimpleName() + " <parquet file> <classification column name>");
			System.exit(1);
		}

		// name of the class label
		String label = args[1];
		
		long start = System.nanoTime();

		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(DatasetClassifier.class.getSimpleName())
				.getOrCreate();

		Dataset<Row> data = spark.read().parquet(args[0]).cache();
		
		int featureCount = 0;
		Object vector = data.first().getAs("features");
		if (vector instanceof DenseVector) {
		   featureCount = ((DenseVector)vector).numActives();
		} else if (vector instanceof SparseVector) {
		   featureCount = ((SparseVector)vector).numActives();
		}
		
		System.out.println("Feature count            : "  + featureCount);
		
		int classCount = (int)data.select(label).distinct().count();
		System.out.println("Class count              : " + classCount);

		System.out.println("Dataset size (unbalanced): " + data.count());
		data.groupBy(label).count().show(classCount);

		data = DatasetBalancer.downsample(data, label, 1);
		
		System.out.println("Dataset size (balanced)  : " + data.count());
		data.groupBy(label).count().show(classCount);

		double testFraction = 0.3;
		long seed = 123;

		SparkMultiClassClassifier mcc;
		Map<String, String> metrics;

		DecisionTreeClassifier dtc = new DecisionTreeClassifier();
		mcc = new SparkMultiClassClassifier(dtc, label, testFraction, seed);
		metrics = mcc.fit(data);
		System.out.println(metrics);

		RandomForestClassifier rfc = new RandomForestClassifier();
		mcc = new SparkMultiClassClassifier(rfc, label, testFraction, seed);
		metrics = mcc.fit(data);
		System.out.println(metrics);

		LogisticRegression lr = new LogisticRegression();
		mcc = new SparkMultiClassClassifier(lr, label, testFraction, seed);
		metrics = mcc.fit(data);
		System.out.println(metrics);

		// specify layers for the neural network
		//    input layer: dimension of feature vector
		//    output layer: number of classes
		int[] layers = new int[] {featureCount, 10, classCount};
		MultilayerPerceptronClassifier mpc = new MultilayerPerceptronClassifier()
				.setLayers(layers)
				.setBlockSize(128)
				.setSeed(1234L)
				.setMaxIter(200);

		mcc = new SparkMultiClassClassifier(mpc, label, testFraction, seed);
		metrics = mcc.fit(data);
		System.out.println(metrics);

		long end = System.nanoTime();

		System.out.println((end-start)/1E9 + " sec");
	}
}
