/**
 * 
 */
package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToInteractingResidues;

/**
 * @author peter
 *
 */
public class Demo8a {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo8a.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local[*]")
	    		  .appName(Demo8a.class.getSimpleName())
	    		  .getOrCreate();
	    
	    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0],  sc);
	    
	    // create a list of all residues with a threshold distance
	    JavaRDD<Row> rows = pdb.flatMap(new StructureToInteractingResidues("CO", 4));
	    
	    Dataset<Row> ds = spark.createDataFrame(rows, StructureToInteractingResidues.getSchema()).cache();

	    System.out.println("Interactions: " + ds.count());
	    
	    // show 10 sample interactions
	    ds.show(10);
	    
	    // list the top 10 residue types that interact with Zn
	    Dataset<Row> counts = ds.groupBy("Res2").count();
	    counts.orderBy(counts.col("count").desc()).show(10);
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
