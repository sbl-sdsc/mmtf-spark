package edu.sdsc.mmtf.spark.applications;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfImporter;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * Imports a local copy of PDB-REDO distribution and saves it as an
 * MMTF-Hadoop Sequence File. 
 * 
 * To create a local copy of PDB-REDO, run the following rsync command:
 * <pre>
 * rsync -av --include='*_final.cif' --exclude='{@literal *}' rsync://rsync.pdb-redo.eu/pdb-redo/{@literal *}/{@literal *}/ pdb-redo/
 * </pre>
 * 
 * @author Peter Rose
 * @since 0.2.0
 */
public class PdbRedoToMmtf {

	public static void main(String[] args) {  
		
		if (args.length != 2) {
			System.err.println("Usage: " + PdbRedoToMmtf.class.getSimpleName() + " <pdb-redo-path> <mmtf-path");
			System.exit(1);
		}
	    
		long start = System.nanoTime();
		
	    // instantiate Spark
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PdbRedoToMmtf.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // import PDB-REDO from a local copy
	    JavaPairRDD<String, StructureDataInterface> pdbredo = MmtfImporter.importPdbRedo(args[0], sc);

	    // save PDB-REDO as an MMTF-Hadoop Sequence file
	    MmtfWriter.writeSequenceFile(args[1], sc, pdbredo);
	    
	    long end = System.nanoTime();
	    
	    System.out.println("time: " + (end-start)/1E9 + " sec.");
	    
	    // close Spark
	    sc.close();
	}
}
