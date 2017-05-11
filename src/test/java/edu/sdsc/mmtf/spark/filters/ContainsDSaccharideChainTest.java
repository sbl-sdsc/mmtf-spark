package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.apps.Demo1b;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

public class ContainsDSaccharideChainTest {
	// should use mmtf files from test project here ...
	private static String path = "/Users/peter/MMTF_Files/full";

	@Test
	public void test3() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    Set<String> pdbIds = new HashSet<String>(Arrays.asList("1STP","1JLP","5X6H","5L2G","2MK1"));
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(path, pdbIds, sc);

	    // 1STP: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and L-DNA chain
	    // 5L2G: L-DNA chain
	    // 2MK1: D-saccharide
	    pdb = pdb.filter(new ContainsDSaccharide());
	    
	    List<String> results = pdb.keys().collect();
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertTrue(results.contains("2MK1"));
	    
	    sc.close();
	}
	
}
