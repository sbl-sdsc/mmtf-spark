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

public class OrFilterTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(OrFilterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 2ONX: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and non-std. DNA chain
	    // 5L2G: DNA chain
	    // 2MK1: D-saccharide
	    // 5UZT: RNA chain (with std. nucleotides)
	    // 1AA6: contains SEC, selenocysteine (21st amino acid)
	    // 1NTH: contains PYL, pyrrolysine (22nd amino acid)
	    List<String> pdbIds = Arrays.asList("2ONX","1JLP","5X6H","5L2G","2MK1","5UZT","1AA6","1NTH");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new OrFilter(new ContainsDnaChain(), new ContainsRnaChain()));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX.A"));
	    assertFalse(results.contains("1JLP.A"));
	    assertFalse(results.contains("5X6H.A"));
	    assertFalse(results.contains("5X6H.B"));
	    assertTrue(results.contains("5L2G.A"));
	    assertTrue(results.contains("5L2G.B"));
	    assertFalse(results.contains("2MK1.A"));
	    assertTrue(results.contains("5UZT.A"));
	    assertFalse(results.contains("1AA6.A"));
	    assertFalse(results.contains("1NTH.A"));
	}
}
