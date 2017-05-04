package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.apps.Demo;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

public class ContainsLProteinChainTest {
	// should use mmtf files from test project here ...
	private static String path = "/Users/peter/MMTF_Files/full";

	@Test
	public void test1() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(path,  sc);

	    // 1STP: only L-protein chain
	    // 5X6H: L-protein and L-DNA chain
	    // 5L2G: L-DNA chain
	    pdb = pdb.filter(t -> Arrays.asList("1STP","5X6H","5L2G").contains(t._1));
	    pdb = pdb.filter(new ContainsLProteinChain());
	    
	    List<String> results = pdb.keys().collect();
	    assertTrue(results.contains("1STP"));
	    assertTrue(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    
	    sc.close();
	}
	
	@Test
	public void test2() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(path,  sc);

	    // 1STP: only L-protein chain
	    // 5X6H: L-protein and L-DNA chain
	    // 5L2G: L-DNA chain
	    pdb = pdb.filter(t -> Arrays.asList("1STP","5X6H","5L2G").contains(t._1));
	    boolean exclusive = true;
	    pdb = pdb.filter(new ContainsLProteinChain(exclusive));
	    
	    List<String> results = pdb.keys().collect();
	    assertTrue(results.contains("1STP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    
	    sc.close();
	}

}
