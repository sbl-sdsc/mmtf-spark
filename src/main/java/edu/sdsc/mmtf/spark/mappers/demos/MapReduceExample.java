package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Counts the number of atoms in the PDB using the classic
 * map-reduce algorithm
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class MapReduceExample {

	/**
	 * Counts the number of atoms in the PDB using the classic
	 * map-reduce algorithm
	 * 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {

	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapReduceExample.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB from MMTF-Hadoop sequence file
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readFullSequenceFile(sc);

	    // count number of atoms
	    long numAtoms = pdb.map(t -> t._2.getNumAtoms()).reduce((a,b) -> a+b);
	    
	    System.out.println("Total number of atoms in PDB: " + numAtoms);
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + (end-start)/1E9 + " sec.");
	    
	    sc.close();
	}
}
