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

import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.StructureToInteractingResidues;

/**
 * @author peter
 *
 */
public class Demo8 {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo8.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local[*]")
	    		  .appName(Demo8.class.getSimpleName())
	    		  .getOrCreate();
	    
	    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);
	    
	    // create a list of all residues with a threshold distance
	    JavaRDD<Row> rows = pdb.flatMap(new StructureToInteractingResidues("ZN", 4));
	    
	    Dataset<Row> ds = spark.createDataFrame(rows, StructureToInteractingResidues.getSchema()).cache();
	    ds.printSchema();
	    ds.show();
	    System.out.println(ds.count());
	    
	    Dataset<Row> counts = ds.groupBy("Res2").count();
	    counts.orderBy(counts.col("count").desc()).show(100);
//	    ds.show();
//	    System.out.println(ds.count());
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
