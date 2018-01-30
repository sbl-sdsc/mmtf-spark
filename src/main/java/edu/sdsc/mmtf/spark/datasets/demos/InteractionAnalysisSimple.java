/**
 * 
 */
package edu.sdsc.mmtf.spark.datasets.demos;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.GroupInteractionExtractor;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class InteractionAnalysisSimple {

	/**
	 * @param args no input arguments
	 * @throws IOException if MmtfReader fails
	 */
	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfFullPath();
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(InteractionAnalysisSimple.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	    
	    // use only representative structures
	    int sequenceIdentity = 40;
	    double resolution = 2.5;
	    pdb = pdb.filter(new Pisces(sequenceIdentity, resolution));
	    
	    GroupInteractionExtractor finder = new GroupInteractionExtractor("ZN", 3);
	    Dataset<Row> interactions = finder.getDataset(pdb).cache();
	    
	    // list the top 10 residue types that interact with Zn
        interactions.printSchema();
        interactions.show(20);
        
        System.out.println("# interactions: " + interactions.count());
        
        // show the top 10 interacting groups
        interactions
        .groupBy(col("residue2"))
        .count()
        .sort(col("count").desc())
        .show(10);
       
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
