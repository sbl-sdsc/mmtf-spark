/**
 * 
 */
package edu.sdsc.mmtf.spark.analysis;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.incubator.StructureToAllInteractions;

/**
 * @author Peter Rose
 *
 */
public class InteractionFinder {
	JavaPairRDD<String, StructureDataInterface> structures;
	
	public InteractionFinder(JavaPairRDD<String, StructureDataInterface> structures) {
		this.structures = structures;
	}

	public Dataset<Row> findInteractingGroups(String groupName, double distance) {
	
	    SparkSession spark = SparkSession
	    		  .builder()
	    		  .master("local[*]")
	    		  .appName(InteractionFinder.class.getSimpleName())
	    		  .getOrCreate();
	    
	    // create a list of all residues with a threshold distance
	    JavaRDD<Row> rows = structures.flatMap(new StructureToAllInteractions(groupName, distance));
	    
	    // convert to a data set
	    Dataset<Row> ds = spark.createDataFrame(rows, StructureToAllInteractions.getSchema()).cache();
	    
	    return ds;
	}

}
