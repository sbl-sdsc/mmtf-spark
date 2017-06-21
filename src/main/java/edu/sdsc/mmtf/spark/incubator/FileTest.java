package edu.sdsc.mmtf.spark.incubator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Structure;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.AdvancedQuery;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;
import scala.Tuple2;

public class FileTest {

	public static void main(String[] args) throws IOException {
		
		String path = System.getProperty("MMTF_REDUCED");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
	
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FileTest.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long start = System.nanoTime();
	  
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
	    
	    pdb.foreach(t -> System.out.println(t._1 + ": " + t._2.getStructureId()));
	}
}
