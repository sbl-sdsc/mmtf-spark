package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class ContainsSequenceRegexTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsSequenceRegexTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 5KE8: contains Zinc finger motif
	    // 1JLP: does not contain Zinc finger motif
	    // 5VAI: contains Walker P loop
	    List<String> pdbIds = Arrays.asList("5KE8","1JLP","5VAI");
	    pdb = MmtfReader.downloadReducedMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new ContainsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("5KE8"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5VAI"));
	}
	
	@Test
	public void test2() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
		pdb = pdb.filter(new ContainsSequenceRegex("C.{2,4}C.{12}H.{3,5}H"));     
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("5KE8.A"));
	    assertFalse(results.contains("5KE8.B"));
	    assertFalse(results.contains("5KE8.C"));
	    assertFalse(results.contains("1JLP.A"));
	    assertFalse(results.contains("5VAI.A"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new ContainsSequenceRegex("[AG].{4}GK[ST]"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("5KE8"));
	    assertFalse(results.contains("1JLP"));
	    assertTrue(results.contains("5VAI"));
	}
}
