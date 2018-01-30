package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.analysis.TraverseStructureHierarchy;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * Converts a directory containing PDB files into an MMTF-Hadoop Sequence file.
 * This class traverses the input directory recursively to find PDB files.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class PdbToMmtfFull {

    /**
     * Converts a directory containing PDB files into an MMTF-Hadoop Sequence file.
     * This class traverses the input directory recursively to find PDB files.
     * 
     * @param args args[0] <path-to-pdb_files>, args[1] <path-to-mmtf-hadoop-file>
     * 
     * @throws FileNotFoundException
     */
	public static void main(String[] args) throws FileNotFoundException {  
		
	    if (args.length != 2) {
	        System.out.println("Usage: PdbToMmtfFull <path-to-pdb_files> <path-to-mmtf-hadoop-file>");
	    }
	    
	    // path to input directory
	    String pdbPath = args[0];
	    
	    // path to output directory
	    String mmtfPath = args[1];
	    
	    long start = System.nanoTime();
	    
	    // instantiate Spark. Each Spark application needs these two lines of code.
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PdbToMmtfFull.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read all PDB file recursively starting the specified directory
	    JavaPairRDD<String, StructureDataInterface> structures = MmtfReader.readPdbFiles(pdbPath, sc);
	    
	    // save a MMTF-Hadoop Sequence File
	    MmtfWriter.writeSequenceFile(mmtfPath, sc, structures);
	    
	    System.out.println(structures.count() + " structures written to: " + mmtfPath);
	    
	    // check results
	    MmtfReader.readSequenceFile(mmtfPath, sc)
	    .foreach(t -> TraverseStructureHierarchy.printChainEntityGroupAtomInfo(t._2));
	    
	    // close Spark
	    sc.close();
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec."); 
	}
}
