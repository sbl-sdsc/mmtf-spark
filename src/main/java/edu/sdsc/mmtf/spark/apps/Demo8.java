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
	    
//	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo5.class.getSimpleName());
//	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local[*]")
	    		  .appName(Demo8.class.getSimpleName())
	    		  .getOrCreate();
	    
	    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0],  sc);

//	    pdb = pdb.filter(t -> t._1.equals("1STP"));
	    
	    JavaRDD<Row> rows = pdb.flatMap(new StructureToInteractingResidues("ADP", 4));
	    
	    Dataset<Row> ds = spark.createDataFrame(rows, StructureToInteractingResidues.getSchema()).cache();
	    ds.printSchema();
	    
	    ds.groupBy("Res2").count().sort("count").show(100);
	    ds.show();
	    System.out.println(ds.count());

//	    System.out.println(list.count());
//	    rows.foreach(t -> System.out.println(t));
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
