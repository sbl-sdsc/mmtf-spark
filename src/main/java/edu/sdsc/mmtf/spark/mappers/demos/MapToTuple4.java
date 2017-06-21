package edu.sdsc.mmtf.spark.mappers.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ExperimentalMethods;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import scala.Tuple4;

/**
 * Example how to map to higher-dimensional tuples.
 * Apache Spark supports Tuple2 ... to Tuple19. This
 * example shows how to map to a Tuple4.
 * See also Dataset as a more scalable alternative to
 * high-dimensional tuples.
 * 
 * @author Peter Rose
 *
 */
public class MapToTuple4 {

	public static void main(String[] args) {

		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapToTuple4.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    // sample a small fraction of the PDB
	    double fraction = 0.001;
	    long seed = 123;
	    
	    MmtfReader
	    		.readSequenceFile(path, fraction, seed, sc)
	    		.filter(new ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))
	    		.map(t -> new Tuple4<String,Float,Float,Float>(t._1, t._2.getResolution(), t._2.getRfree(), t._2.getRwork())) 
	    		.foreach(t -> System.out.println(t));
	    
	    sc.close();
	}
}
