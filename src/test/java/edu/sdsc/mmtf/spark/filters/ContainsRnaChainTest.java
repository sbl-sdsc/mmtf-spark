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

public class ContainsRnaChainTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsRnaChainTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	 // 2ONX: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and DNA chain
	    // 5L2G: DNA chain
	    // 2MK1: D-saccharide
	    // 5UX0: 2 L-protein, 2 RNA, 2 DNA chains
	    // 2NCQ: 2 RNA chains
	    List<String> pdbIds = Arrays.asList("2ONX","1JLP","5X6H","5L2G","2MK1","5UX0","2NCQ");
	    pdb = MmtfReader.downloadReducedMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new ContainsRnaChain());    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertTrue(results.contains("5UX0"));
        assertTrue(results.contains("2NCQ"));
	}
	
	@Test
	public void test2() {
	    boolean exclusive = true;
	    pdb = pdb.filter(new ContainsRnaChain(exclusive));   
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertFalse(results.contains("5UX0"));
        assertTrue(results.contains("2NCQ"));
	}
	
	@Test
	public void test3() {
        pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new ContainsRnaChain());    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX.A"));
	    assertFalse(results.contains("1JLP.A"));
	    assertFalse(results.contains("5X6H.A"));
	    assertFalse(results.contains("5X6H.B"));
	    assertFalse(results.contains("5L2G.A"));
	    assertFalse(results.contains("5L2G.B"));
	    assertFalse(results.contains("2MK1.A"));
	    assertFalse(results.contains("5UX0.A"));
	    assertFalse(results.contains("5UX0.D"));
	    assertTrue(results.contains("5UX0.B"));
	    assertTrue(results.contains("5UX0.E"));
	    assertFalse(results.contains("5UX0.C"));
	    assertFalse(results.contains("5UX0.F"));
        assertTrue(results.contains("2NCQ.A"));
        assertTrue(results.contains("2NCQ.S"));
	}
}
