/**
 * 
 */
package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.GroupInteractionExtractor;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;

/**
 * @author Peter Rose
 *
 */
public class AtpInteractionAnalysis {

	/**
	 * @param args input arguments
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	     
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(AtpInteractionAnalysis.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	   
	    // filter by sequence identity subset
	    pdb = pdb.filter(new BlastClusters(40));
	    
	    // find ATP interactions within 3 Angstroms
	    GroupInteractionExtractor finder = new GroupInteractionExtractor("ATP", 3);
	    Dataset<Row> interactions = finder.getDataset(pdb).cache();
	    
	    // TODO add a line to only analyze interactions 
	    // with the oxygens in the terminal phosphate group of ATP
	    // (O1G, O2G, O3G)
	    // Tip: Google SQL LIKE
	    interactions = interactions.filter("atom1 LIKE('O%G')");
	    
	    // show the data schema of the dataset and some data
        interactions.printSchema();
        interactions.show(20);
        
        long n = interactions.count();
        System.out.println("# interactions: " + n);
        
        System.out.println("Top interacting groups");

        Dataset<Row> topGroups = interactions
        		.groupBy("residue2")
        		.count();
        
        topGroups
        .sort(col("count").desc()) // sort descending by count
        .show(10);
        
        System.out.println("Top interacting group/atoms types");
 
        Dataset<Row> topGroupsAndAtoms = interactions
        		.groupBy("residue2","atom2")
        		.count();

        topGroupsAndAtoms
        .withColumn("frequency", col("count").divide(n)) // add column with frequency of occurrence
        .sort(col("frequency").desc()) // sort descending
        .show(10);

        long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
