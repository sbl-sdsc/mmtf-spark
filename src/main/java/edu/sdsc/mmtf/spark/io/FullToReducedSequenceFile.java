/**
 * 
 */
package edu.sdsc.mmtf.spark.io;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.ReducedEncoder;

import scala.Tuple2;

/**
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FullToReducedSequenceFile {

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfFullPath();
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullToReducedSequenceFile.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
	    		.readSequenceFile(path, sc)
	    		.mapToPair(t -> new Tuple2<String,StructureDataInterface>(t._1, ReducedEncoder.getReduced(t._2)));
    
	    path = MmtfReader.getMmtfReducedPath();

	    MmtfWriter.writeSequenceFile(path, sc, pdb);
	    
	    System.out.println("# structures: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
