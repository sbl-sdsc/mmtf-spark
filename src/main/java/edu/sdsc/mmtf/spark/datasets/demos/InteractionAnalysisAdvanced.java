/**
 * 
 */
package edu.sdsc.mmtf.spark.datasets.demos;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.kurtosis;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

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
 */
public class InteractionAnalysisAdvanced {

	/**
	 * @param args no input arguments
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String path = MmtfReader.getMmtfFullPath();
	     
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(InteractionAnalysisAdvanced.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	   
	    // get non-redundant subset
	    pdb = pdb.filter(new Pisces(40, 2.5));
	    
	    // find Zinc interactions within 3 Angstroms
	    GroupInteractionExtractor finder = new GroupInteractionExtractor("ZN", 3);
	    Dataset<Row> interactions = finder.getDataset(pdb).cache();
	    
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
        		.filter("element2 != 'C'") // exclude carbon interactions
        		.groupBy("residue2","atom2")
        		.count();

        topGroupsAndAtoms
        .withColumn("frequency", col("count").divide(n)) // add column with frequency of occurrence
        .filter("frequency > 0.01") // filter out occurrences < 1 %
        .sort(col("frequency").desc()) // sort descending
        .show(20);

        // TODO print the top 10 interacting elements
        System.out.println("Top interacting elements");
        Dataset<Row> topElements = interactions
        		.filter("element2 != 'C'") // exclude carbon interactions
        		.groupBy("element2")
        		.count();
        
        topElements.withColumn("frequency", col("count").divide(n))
        .filter("frequency > 0.01") // filter out occurrences < 1 %
        .sort(col("frequency").desc()) // sort descending
        .show(10);

        interactions
        .groupBy("element2")
        .avg("distance")
        .sort("avg(distance)")
        .show(10);

        // Aggregate multiple statistics
        // Note: import static org.apache.spark.sql.functions.* required!
        // e.g. org.apache.spark.sql.functions.avg
        // for a list of all available functions
        interactions
        .groupBy("element2")
        .agg(count("distance"),avg("distance"),min("distance"),max("distance"),kurtosis("distance"))
        .show(10);
        
        long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
