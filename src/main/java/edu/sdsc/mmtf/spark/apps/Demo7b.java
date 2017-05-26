/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.analysis.InteractionFinder;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * @author peter
 *
 */
public class Demo7b {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo7b.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo7b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    double fraction = 0.1;
	    long seed = 1;
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0], fraction, seed, sc);
	    
	    InteractionFinder finder = new InteractionFinder(pdb);
	    Dataset<Row> interactions = finder.findInteractingGroups("ZN", 3).cache();
	    
	    // list the top 10 residue types that interact with Zn
        interactions.printSchema();
        interactions.show(20);
        System.out.println("# interactions: " + interactions.count());
        interactions.groupBy(interactions.col("Res2")).count().sort("count").show(1000);
        interactions.groupBy(interactions.col("Res2")).avg("Dist").sort("avg(Dist)").show(1000);

	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
