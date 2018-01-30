package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureElementExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;

/**
 * This class creates a dataset of helical sequence segments derived
 * from a non-redundant set. The dataset contains the sequence segment,
 * the DSSP Q8 and DSSP Q3 code of the center residue in a sequence
 * segment, and a Word2Vector encoding of the sequence segment.
 * The dataset is saved in a file specified by the user.
 * 
 * @author Peter Rose
 * @since 0.1.0
 */
public class SecondaryStructureElementsWord2VecEncoder {
	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfReducedPath();
	    
		if (args.length != 2) {
			System.err.println("Usage: " + SecondaryStructureWord2VecEncoder.class.getSimpleName() + " <outputFilePath> + <fileFormat>");
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
			
		// extract helical sequence segments
		Dataset<Row> data = SecondaryStructureElementExtractor.getDataset(pdb, "H");
		System.out.println(data.count());
		data.show(10,false);
		
		int segmentLength = 11;
		
		// add Word2Vec encoded feature vector
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(data);
		int n = 2;
		int windowSize = (segmentLength-1)/2;
		int vectorSize = 50;
		data = encoder.overlappingNgramWord2VecEncode(n, windowSize, vectorSize);	
		
		if (args[1].equals("json")) {
			// coalesce data into a single file
		    data = data.coalesce(1);
		}
		
		data.show(50,false);
		
		data.write().mode("overwrite").format(args[1]).save(args[0]);
		
		long end = System.nanoTime();

		System.out.println(TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	}
}
