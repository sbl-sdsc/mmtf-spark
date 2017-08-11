package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.PolymerSequenceExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;

/**
 * This class generates Word2Vector models from protein sequences
 * in the PDB using an overlapping n-grams.
 * 
 * @author Peter Rose
 *
 */
public class PdbSequenceToWord2Vec {
	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Path for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
		if (args.length != 1) {
			System.err.println("Usage: " + SecondaryStructureWord2VecEncoder.class.getSimpleName() + " <outputFileName>");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(SecondaryStructureWord2VecEncoder.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		double fraction = 1.0;
		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, sc)
				.flatMapToPair(new StructureToPolymerChains(false,true))
				.filter(new ContainsLProteinChain()) // filter out for example D-proteins
                .sample(false, fraction, seed);
			
		Dataset<Row> data = PolymerSequenceExtractor.getDataset(pdb);
		data.show(10,false);
		
		int segmentLength = 11;
		
		// add Word2Vec encoded feature vector
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(data);
		int n = 2;
		int windowSize = (segmentLength-1)/2;
		int vectorSize = 50;
		data = encoder.overlappingNgramWord2VecEncode(n, windowSize, vectorSize);	
		
		encoder.getWord2VecModel().save(args[0]);
		
		long end = System.nanoTime();

		System.out.println(TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	}
}
