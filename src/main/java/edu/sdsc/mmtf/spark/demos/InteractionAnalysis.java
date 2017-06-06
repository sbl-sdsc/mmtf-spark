/**
 * 
 */
package edu.sdsc.mmtf.spark.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.analysis.InteractionFinder;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;

/**
 * @author Peter Rose
 *
 */
public class InteractionAnalysis {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

	    if (args.length != 1) {
	        System.err.println("Usage: " + InteractionAnalysis.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(InteractionAnalysis.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    double fraction = 1.0;
	    long seed = 1;
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0], fraction, seed, sc);
	   
	    // filter by sequence identity subset
	    pdb = pdb.filter(new BlastClusters(40));
	    
	    // find interactions between Zinc within 3 Angstroms
	    InteractionFinder finder = new InteractionFinder(pdb);
	    Dataset<Row> interactions = finder.findInteractingGroups("ZN", 3).cache();
	    
	    // show the data schema of the dataset and some data
        interactions.printSchema();
        interactions.show(20);
        
        long n = interactions.count();
        System.out.println("# interactions: " + interactions.count());
        
        System.out.println("Top interacting group types");
        interactions.groupBy(interactions.col("Res2")).count().sort("count").show(25);
        
        System.out.println("Top interacting group/atoms types");
        Dataset<Row> subset1 = interactions.groupBy(interactions.col("Res2"), interactions.col("Atom2")).count().sort("count");
        subset1.withColumn("freq", subset1.col("count").divide(n)).filter("freq > 0.01").show(1000);
        
        System.out.println("Top interacting elements");
        Dataset<Row> subset2 = interactions.groupBy(interactions.col("Element2")).count().sort("count");
        subset2.withColumn("freq", subset2.col("count").divide(n)).filter("freq > 0.01").show(100);
        
        System.out.println("Top interacting group/elements");
        Dataset<Row> subset3 = interactions.groupBy(interactions.col("Res2"),interactions.col("Element2")).count().sort("count");
        subset3.withColumn("freq", subset3.col("count").divide(n)).filter("freq > 0.01").show(100);
        
        interactions.groupBy(interactions.col("Element2")).avg("Dist").sort("avg(Dist)").show(100);

        long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
