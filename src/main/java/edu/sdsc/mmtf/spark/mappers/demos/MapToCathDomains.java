package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.demos.TraverseStructureHierarchy;
import edu.sdsc.mmtf.spark.mappers.StructureToCathDomains;

/**
 * COMMENT TODO
 * 
 * @author Yue Yu
 * @since 0.2.0
 *
 */
public class MapToCathDomains {

	
	
	
	public static void main(String[] args) throws IOException {

	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapToCathDomains.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);

//	    List<String> pdbIds = Arrays.asList("1HV4");
	    List<String> pdbIds = Arrays.asList("1STQ");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
//	    String baseUrl = "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/daily-release/newest/cath-b-newest-all.gz";
	   
	    pdb = pdb.flatMapToPair(new StructureToCathDomains(StructureToCathDomains.CATH_B_NEWEST_ALL));
	    
	    pdb.foreach(t -> TraverseStructureHierarchy.printAll(t._2));
	   
	    System.out.println("# cathDomains in 1HV4: " + pdb.count());
	    
	    sc.close();
	}
}
