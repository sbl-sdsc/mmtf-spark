/**
 * 
 */
package edu.sdsc.mmtf.spark.ml.demos;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.biojava.nbio.structure.StructureException;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * This class is a simple example of using Dataset operations to create a dataset
 * 
 * @author Peter Rose
 *
 */
public class ProteinFoldDatasetCreator {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws StructureException 
	 */
	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
		if (args.length != 1) {
			System.err.println("Usage: " + ProteinFoldDatasetCreator.class.getSimpleName() + " <dataset output file");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(ProteinFoldDatasetCreator.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// read MMTF Hadoop sequence file and create a non-redundant set (<=40% seq. identity)
		// of L-protein chains
		int sequenceIdentity = 20;
		double resolution = 3.0;
		double fraction = 1.0;
		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, fraction, seed, sc)
				.flatMapToPair(new StructureToPolymerChains())
                .filter(new Pisces(sequenceIdentity, resolution))
				.filter(new ContainsLProteinChain()); // filter out for example D-proteins

		// get secondary structure content
		Dataset<Row> data = SecondaryStructureExtractor.getDataset(pdb);

		// classify chains by secondary structure type
		double minThreshold = 0.05;
		double maxThreshold = 0.15;
     	data = addProteinFoldType(data, minThreshold, maxThreshold);
     	
     	// create a binary classification dataset
		data = data.filter("foldType = 'alpha' OR foldType = 'beta'").cache();

		// create a three-state classification model (alpha, beta, alpha+beta)
		//		data = data.filter("foldType != 'other'").cache();

		// add Word2Vec encoded feature vector
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(data);
		int n = 2;
		int windowSize = 11;
		int vectorSize = 50;
		data = encoder.overlappingNgramWord2VecEncode(n, windowSize, vectorSize);	

		data.printSchema();
		data.show(25);
		
		// keep only a subset of relevant fields for further processing
        data = data.select("structureChainId", "alpha", "beta", "coil", "foldType", "features");
	
        data.write().mode("overwrite").format("parquet").save(args[0]);
		
		long end = System.nanoTime();

		System.out.println((end-start)/1E9 + " sec");
	}
	
	/**
	 * Adds a column "foldType" with three major secondary structure classes: 
	 * "alpha", "beta", "alpha+beta", and "other" based upon the fraction of alpha/beta content.
	 * 
	 * The simplified syntax used in this method relies on two static imports:
	 * import static org.apache.spark.sql.functions.when;
     * import static org.apache.spark.sql.functions.col;
     * 
	 * @param data input dataset with alpha, beta composition
	 * @param minThreshold below this threshold, the secondary structure type is ignored
	 * @param maxThreshold above this threshold, the secondary structure type is assigned
	 * @return
	 */
	public static Dataset<Row> addProteinFoldType(Dataset<Row> data, double minThreshold, double maxThreshold) {
		return data.withColumn("foldType",
				when(col("alpha").gt(maxThreshold).and(col("beta").lt(minThreshold)), "alpha")
				.when(col("beta").gt(maxThreshold).and(col("alpha").lt(minThreshold)), "beta")
				.when(col("alpha").gt(maxThreshold).and(col("beta").gt(maxThreshold)), "alpha+beta")
				.otherwise("other")
				);
	}
	
	/**
	 * Alternative method using an SQL User defined functions to create label column
	 * @param data
	 * @return
	 */
	public static Dataset<Row> createLabelWithUdf(Dataset<Row> data) {
		SparkSession session = data.sparkSession();
		session.udf().register("alpha", (Float a, Float b) -> (a > 0.15 && b < 0.05? "alpha":""), DataTypes.StringType);
		session.udf().register("beta", (Float a, Float b) -> (b > 0.15 && a < 0.05? "beta":""), DataTypes.StringType);
		session.udf().register("alphabeta", (Float a, Float b) -> (a > 0.15 && b > 0.15? "alpha+beta":""), DataTypes.StringType);
	
		data.createOrReplaceTempView("table");
		data = session.sql("SELECT *, "
				+ "CONCAT(alpha(alpha, beta),beta(alpha, beta),alphabeta(alpha,beta)) AS foldType from table");
        return data;
	}
}
