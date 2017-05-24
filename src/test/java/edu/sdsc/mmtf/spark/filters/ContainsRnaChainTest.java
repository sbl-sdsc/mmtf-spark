package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.apps.Demo1b;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class ContainsRnaChainTest {

	@Test
	public void test1() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    List<String> pdbIds = Arrays.asList("1STP","1JLP","5X6H","5L2G","2MK1","5UX0","2NCQ");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);

	    // 1STP: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and DNA chain
	    // 5L2G: DNA chain
	    // 2MK1: D-saccharide
	    // 5UX0: 2 L-protein, 2 RNA, 2DNA chains
	    // 2NCQ: 2 RNA chains
	    pdb = pdb.filter(new ContainsRnaChain());    
	    List<String> results = pdb.keys().collect();
	    sc.close();
	    
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertTrue(results.contains("5UX0"));
        assertTrue(results.contains("2NCQ"));
	}
	
	@Test
	public void test2() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    List<String> pdbIds = Arrays.asList("1STP","1JLP","5X6H","5L2G","2MK1","5UX0","2NCQ");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);

	    // 1STP: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and DNA chain
	    // 5L2G: DNA chain
	    // 2MK1: D-saccharide
	    // 5UX0: 2 L-protein, 2 RNA, 2DNA chains
	    // 2NCQ: 2 RNA chains
	    boolean exclusive = true;
	    pdb = pdb.filter(new ContainsRnaChain(exclusive));   
	    List<String> results = pdb.keys().collect();
	    sc.close();
	    
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertFalse(results.contains("5UX0"));
        assertTrue(results.contains("2NCQ"));
	}
	
	@Test
	public void test3() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    List<String> pdbIds = Arrays.asList("1STP","1JLP","5X6H","5L2G","2MK1","5UX0","2NCQ");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
        pdb = pdb.flatMapToPair(new StructureToPolymerChains());
        
	    // 1STP: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and DNA chain
	    // 5L2G: DNA chain
	    // 2MK1: D-saccharide
	    // 5UX0: 2 L-protein, 2 RNA, 2DNA chains
	    // 2NCQ: 2 RNA chains
	    pdb = pdb.filter(new ContainsRnaChain());    
	    List<String> results = pdb.keys().collect();
	    sc.close();
	    
	    assertFalse(results.contains("1STP.A"));
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
