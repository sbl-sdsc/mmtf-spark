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
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * This class creates a dataset of sequence segments derived
 * from a non-redundant set. The dataset contains the sequence segment,
 * the DSSP Q8 and DSSP Q3 code of the center residue in a sequence
 * segment, and a Word2Vec encoded feature vector using
 * a pre-trained Word2Vec model read from file.
 * The dataset is saved in a file specified by the user.
 * 
 * @author Yue Yu
 * @since 0.1.0
 */
public class SecondaryStructureWord2VecModelEncoder {

	/**
	 * @param args args[0] outputFilePath, args[1] outputFormat (json|parquet), args[3] word2VecModelFile
	 * @throws IOException 
	 * @throws StructureException 
	 */
	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfFullPath();
	    
		if (args.length != 3) {
			System.err.println("Usage: " + SecondaryStructureWord2VecModelEncoder.class.getSimpleName() + " <outputFilePath> + <fileFormat> + <word2VecModelFile>");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkConf conf = new SparkConf()
				.setMaster("local[*]")
				.setAppName(SecondaryStructureWord2VecModelEncoder.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// read MMTF Hadoop sequence file and create a non-redundant Pisces 
		// subset set (<=20% seq. identity) of L-protein chains
		int sequenceIdentity = 20;
		double resolution = 3.0;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.readSequenceFile(path, sc)
				.flatMapToPair(new StructureToPolymerChains())
                .filter(new Pisces(sequenceIdentity, resolution));
		
		// get content
		int segmentLength = 11; 
		Dataset<Row> data = SecondaryStructureSegmentExtractor.getDataset(pdb, segmentLength);
	
		// add Word2Vec encoded feature vector using
		// a pre-trained Word2Vec model read from file
		ProteinSequenceEncoder encoder = new ProteinSequenceEncoder(data);
		int n = 2;
		String modelFileName = args[2];
		data = encoder.overlappingNgramWord2VecEncode(modelFileName, n).cache();
		
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
