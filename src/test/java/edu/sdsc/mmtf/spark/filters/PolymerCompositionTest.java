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

public class PolymerCompositionTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PolymerCompositionTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 1STP: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and DNA chain (with std. nucleotides)
	    // 5L2G: DNA chain (with non-std. nucleotide)
	    // 2MK1: D-saccharide
	    // 5UZT: RNA chain (with std. nucleotides)
	    // 1AA6: contains SEC, selenocysteine (21st amino acid)
	    // 1NTH: contains PYL, pyrrolysine (22nd amino acid)
	    List<String> pdbIds = Arrays.asList("1STP","1JLP","5X6H","5L2G","2MK1","5UZT","1AA6","1NTH");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertTrue(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertFalse(results.contains("5UZT"));
	    assertFalse(results.contains("1AA6"));
	    assertFalse(results.contains("1AA6"));
	    assertFalse(results.contains("1NTH"));
	}
	
	@Test
	public void test2() {
	    boolean exclusive = true;
	    pdb = pdb.filter(new PolymerComposition(exclusive, PolymerComposition.AMINO_ACIDS_20));   
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertFalse(results.contains("5UZT"));
	    assertFalse(results.contains("1AA6"));
	    assertFalse(results.contains("1NTH"));
	}
	
	@Test
	public void test3() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_20));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1STP.A"));
	    assertFalse(results.contains("1JLP.A"));
	    assertTrue(results.contains("5X6H.B"));
	    assertFalse(results.contains("5L2G.A"));
	    assertFalse(results.contains("5L2G.B"));
	    assertFalse(results.contains("2MK1.A"));
	    assertFalse(results.contains("5UZT.A"));
	    assertFalse(results.contains("1AA6.A"));
	    assertFalse(results.contains("1NTH.A"));
	}
	
	@Test
	public void test4() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new PolymerComposition(PolymerComposition.AMINO_ACIDS_22));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1STP.A"));
	    assertFalse(results.contains("1JLP.A"));
	    assertTrue(results.contains("5X6H.B"));
	    assertFalse(results.contains("5L2G.A"));
	    assertFalse(results.contains("5L2G.B"));
	    assertFalse(results.contains("2MK1.A"));
	    assertFalse(results.contains("5UZT.A"));
	    assertTrue(results.contains("1AA6.A"));
	    assertTrue(results.contains("1NTH.A"));
	}
	
	@Test
	public void test5() {
	    pdb = pdb.filter(new PolymerComposition(PolymerComposition.DNA_STD_NUCLEOTIDES));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertTrue(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertFalse(results.contains("5UZT"));
	}
	
	@Test
	public void test6() {
	    pdb = pdb.filter(new PolymerComposition(PolymerComposition.RNA_STD_NUCLEOTIDES));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertTrue(results.contains("5UZT"));
	}
	
	@Test
	public void test7() {
	    pdb = pdb.filter(new PolymerComposition(PolymerComposition.RNA_STD_NUCLEOTIDES));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	    assertTrue(results.contains("5UZT"));
	}
}
