package edu.sdsc.mmtf.spark.ml.demos;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.UniProt;
import edu.sdsc.mmtf.spark.datasets.UniProt.UniProtDataset;
import edu.sdsc.mmtf.spark.ml.ProteinSequenceEncoder;

/**
 * This class generates Word2Vector models from protein sequences
 * in UniProt using overlapping n-grams.
 * 
 * @author Peter Rose
 *
 */
public class SwissProtSequenceToWord2Vec2 {
	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			System.err.println("Usage: " + SwissProtSequenceToWord2Vec2.class.getSimpleName() + " <outputFileName>");
			System.exit(1);
		}

		long start = System.nanoTime();

		SparkSession.builder().master("local[*]").getOrCreate();
		
		Dataset<Row> data = UniProt.getDataset(UniProtDataset.SWISS_PROT);
		
		// make sure there are no empty sequence records
		data = data.na().drop(new String[]{"sequence"});
		
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
