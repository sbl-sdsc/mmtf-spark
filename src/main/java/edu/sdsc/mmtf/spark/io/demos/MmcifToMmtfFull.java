package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfImporter;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * Converts a directory containing .cif files into an MMTF-Hadoop Sequence file
 * with "full" (all atom, full precision) representation. The input directory 
 * is traversed recursively to find .cif files.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class MmcifToMmtfFull {

    /**
     * Converts a directory containing .cif files into an MMTF-Hadoop Sequence file.
     * The input directory is traversed recursively to find PDB files.
     * 
     * @param args args[0] <input-path-to-cif_files>, args[1] <output-path-to-mmtf-hadoop-file>
     * 
     * @throws FileNotFoundException
     */
	public static void main(String[] args) throws FileNotFoundException {  
		
	    if (args.length != 2) {
	        System.out.println("Usage: MmcifToMmtfFull <input-path-to-cif_files> <output-path-to-mmtf-hadoop-file>");
	    }
	    
	    // path to input directory
	    String cifPath = args[0];
	    
	    // path to output directory
	    String mmtfPath = args[1];

	    // instantiate Spark
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("MmcifToMmtfFull");
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read cif files recursively starting from the specified top level directory
	    JavaPairRDD<String, StructureDataInterface> structures = MmtfImporter.importMmcifFiles(cifPath, sc);
	    
	    // save as an MMTF-Hadoop Sequence File
	    MmtfWriter.writeSequenceFile(mmtfPath, sc, structures);
	    
	    System.out.println(structures.count() + " structures written to: " + mmtfPath);
	    
	    // close Spark
	    sc.close(); 
	}
}
