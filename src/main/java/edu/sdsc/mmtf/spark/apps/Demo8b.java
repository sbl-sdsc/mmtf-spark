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
public class Demo8b {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo8b.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local[*]")
	    		  .appName(Demo8b.class.getSimpleName())
	    		  .getOrCreate();
	    
	    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(args[0],  sc);
	    
	    // create a list of all residues with a threshold distance
	    JavaRDD<Row> rowsCu = pdb.flatMap(new StructureToInteractingResidues("CU", 4));
	    
	    Dataset<Row> dsCu = spark.createDataFrame(rowsCu, StructureToInteractingResidues.getSchema()).cache();
	    
	    // create a list of all residues with a threshold distance
	    JavaRDD<Row> rowsZn = pdb.flatMap(new StructureToInteractingResidues("ZN", 4));
	    
	    Dataset<Row> dsZn = spark.createDataFrame(rowsZn, StructureToInteractingResidues.getSchema()).cache();

        Dataset<Row> union = dsCu.join(dsZn, dsCu.col("Res2").equalTo(dsZn.col("Res2"))).cache();
	    // list the top 10 residue types that interact with Zn
        union.printSchema();
        union.show(20);
        union.groupBy(union.col("Res2"));
	    union.orderBy(union.col("count").desc()).show(10);
	  
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
