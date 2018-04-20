package edu.sdsc.mmtf.spark.alignments.demos;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.alignments.StructureAligner;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * This class demonstrates how to run an all vs. all
 * structural alignment of a set of protein chains.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class DemoAllVsAll {

	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfReducedPath();

		long start = System.nanoTime();
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(DemoAllVsAll.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read PDB and create a Pisces non-redundant set at 20% sequence identity and a resolution better than 1.6 A.
		// Then take a 1% random sample.	
		double fraction = 0.01;
		
		// optional command line argument
		if (args.length == 1) {
			fraction = Double.parseDouble(args[0]);
		}

		long seed = 123;
		
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc)
				.flatMapToPair(new StructureToPolymerChains())
				.filter(new Pisces(20, 1.6))
				.sample(false, fraction, seed);
		
		System.out.println(pdb.count());
		// run the structural alignment
		String algorithmName = FatCatRigid.algorithmName;
		Dataset<Row> alignments = StructureAligner.getAllVsAllAlignments(pdb, algorithmName).cache();
		
		// show results
	    int count = (int)alignments.count();		
		alignments.show(count);
		
        System.out.println("Pairs: " + count);
		
		long end = System.nanoTime();
		
		System.out.println("Time per alignment: " + TimeUnit.NANOSECONDS.toMillis((end-start)/count) + " msec.");
		
		System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
		
		sc.close();
	}
}
