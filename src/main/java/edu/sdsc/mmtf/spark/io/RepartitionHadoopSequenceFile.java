package edu.sdsc.mmtf.spark.io;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

/**
 * This class repartitions an MMTF-Hadoop Sequence file. Many small partitions
 * will lead to inefficient processing. This class can be used to repartition an
 * MMTF-Hadoop Sequence file to fewer partitions.
 * 
 * @author Peter Rose
 * @since 0.2.0
 */
public class RepartitionHadoopSequenceFile {

	/**
	 * Reparations an MMTF-Hadoop Sequence file.
	 * 
	 * @param args
	 *            args[0] path to input Hadoop Sequence file, args[1] path to
	 *            output Hadoop Sequence File, args[3] number of partitions
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(RepartitionHadoopSequenceFile.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		long start = System.nanoTime();

		if (args.length != 3) {
			System.out.println("Usage: RepartitionHadoopSequenceFile <input-path> <ouput-path> <number-of-partitions>");
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		int numPartitions = Integer.parseInt(args[2]);

		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(inputPath, sc);
		pdb = pdb.repartition(numPartitions);
		MmtfWriter.writeSequenceFile(outputPath, sc, pdb);

		long end = System.nanoTime();
		System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + " sec.");

		sc.close();
	}
}
