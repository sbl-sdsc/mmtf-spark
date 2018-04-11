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

	    List<String> pdbIds = Arrays.asList("1HV4"); // single protein chain 5IBZ -> D2
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
	    String baseUrl = "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/daily-release/newest/cath-b-newest-all.gz";
	    HashMap<String, ArrayList<String>> hmap = StructureToCathDomains.getMap(baseUrl);
	   
	    pdb = pdb.flatMapToPair(new StructureToCathDomains(hmap));
	   
	    System.out.println("# cathDomains in 1HV4: " + pdb.count());
	    
	    sc.close();
	}
}
