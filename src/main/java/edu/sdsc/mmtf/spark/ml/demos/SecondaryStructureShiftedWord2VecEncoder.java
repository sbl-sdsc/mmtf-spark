/**
 * 
 */
package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.StructureException;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureSegmentExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

/**
 * This class creates a dataset of sequence segments derived
 * from a non-redundant set. The dataset contains the sequence segment,
 * the DSSP Q8 and DSSP Q3 code of the center residue in a sequence
 * segment, and a 3-gram shifted Word2Vec encoding of the sequence segment.
 * The dataset is saved in JSON file specified by the user.
 * 
 * @author Peter Rose
 */
public class SecondaryStructureShiftedWord2VecEncoder {

	/**
	 * @param args outputFilePath outputFormat (json|parquet)
	 * @throws IOException 
	 * @throws StructureException 
	 */
	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Path for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
		if (args.length != 2) {
			System.err.println("Usage: " + SecondaryStructureShiftedWord2VecEncoder.class.getSimpleName() + " <outputFilePath> + <fileFormat>");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(SecondaryStructureShiftedWord2VecEncoder.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// read MMTF Hadoop sequence file and create a non-redundant set (<=20% seq. identity)
		// of L-protein chains
		int sequenceIdentity = 20;
		double resolution = 2.0;
		double fraction = 1.0;
		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new Pisces(sequenceIdentity, resolution)) // The pisces filter
				.filter(new ContainsLProteinChain()) // filter out for example D-proteins
                .sample(false,  fraction, seed);
		
		// get content
		int segmentLength = 25; 
		Dataset<Row> data = SecondaryStructureSegmentExtractor.getDataset(pdb, segmentLength);
	
		// create a Word2Vector representation of the protein sequences
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(data);
		int windowSize = (segmentLength-1)/2;
		int vectorSize = 50; // dimension of feature vector	(50)
		data = encoder.shifted3GramWord2VecEncode(windowSize, vectorSize).cache();
	
		data.printSchema();
		data.show(25, false);
		
		if (args[1].equals("json")) {
			// coalesce data into a single file
		    data = data.coalesce(1);
		}
		data.write().mode("overwrite").format(args[1]).save(args[0]);
		
		long end = System.nanoTime();

		System.out.println(TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	}
}
