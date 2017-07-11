/**
 * 
 */
package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.StructureException;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SequenceSegmentsExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.SequenceWord2Vector;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

/**
 * This class is a simple example of using Dataset operations to create a dataset
 * And this class will generate a JSon file in a directory the user passes in
 * 
 * @author Yue Yu
 */
public class SecondaryStructureDatasetCreator {

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
			System.err.println("Usage: " + SecondaryStructureDatasetCreator.class.getSimpleName() + " <dataset output file");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(SecondaryStructureDatasetCreator.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// read MMTF Hadoop sequence file and create a non-redundant set (<=20% seq. identity)
		// of L-protein chains
		int sequenceIdentity = 20;
		double resolution = 2.0;
		double fraction = 0.1;
		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, fraction, seed, sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new Pisces(sequenceIdentity, resolution)) // The pisces filter
				.filter(new ContainsLProteinChain()); // filter out for example D-proteins

		// get content
		Dataset<Row> data = SequenceSegmentsExtractor.getDataset(pdb);
		// create a Word2Vector representation of the protein sequences
		int n = 2; // create 2-grams
		int windowSize = 25; // 25-amino residue window size for Word2Vector
		int vectorSize = 50; // dimension of feature vector	
		
		data = SequenceWord2Vector.addFeatureVector(data, n, windowSize, vectorSize).cache();
		data.show(25, false);
		data.toJSON().write().mode("overwrite").format("json").save(args[0]);
		

		long end = System.nanoTime();

		System.out.println((end-start)/1E9 + " sec");
	}
	

}
