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

/**
 * Converts a full MMTF Hadoop Sequence File to a reduced MMTF representation.
 * MMTF representations: 
 * <p><ul>
 * <li>reduced: C-alpha atoms for polypeptides, P for polynucleotides, 
 * and all atom for all other groups (residues) at 0.1 A coordinate precision;
 * <li>full: all atoms at 0.001 A coordinate precision
 * </ul>
 * 
 * <p> Reference: Bradley AR, Rose AS, Pavelka A, Valasatava Y, Duarte JM, Prlić A, Rose PW (2017) 
 * MMTF - an efficient file format for the transmission, visualization, and analysis of macromolecular 
 * structures. PLOS Computational Biology 13(6): e1005575. <a href="https://doi.org/10.1371/journal.pcbi.1005575">doi: 10.1371/journal.pcbi.1005575</a>
 * 
 * @author Peter Rose
 * @since 0.1.0
 * @see <a href="https://mmtf.rcsb.org/download.html">MMTF Website</a>
 *
 */
public class FullToReducedSequenceFile {

    /**
     * Converts a full MMTF Hadoop Sequence File to a reduced representation.
     * @param args args[0] input directory (full), 
     * args[1] output directory (reduced)
     * @throws FileNotFoundException
     */
	public static void main(String[] args) throws FileNotFoundException {
	    if (args.length != 2) {
	        System.out.println("Usage: FullToReducedSequenceFile <path_to_full> <path_to_reduced>");
	        System.exit(-1);
	    }

		String fullPath = args[0];
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FullToReducedSequenceFile.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
	    		.readSequenceFile(fullPath, sc)
	    		.mapValues(s -> ReducedEncoder.getReduced(s));
	    		
	    String reducedPath = args[1];

	    MmtfWriter.writeSequenceFile(reducedPath, sc, pdb);
	    
	    System.out.println("# structures converted: " + pdb.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
