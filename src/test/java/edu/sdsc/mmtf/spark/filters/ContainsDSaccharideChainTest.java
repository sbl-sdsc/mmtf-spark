package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.assertFalse;

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

public class ContainsDSaccharideChainTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsDSaccharideChainTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 2ONX: only L-protein chain
	    // 1JLP: single L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: L-protein and L-DNA chain
	    // 5L2G: L-DNA chain
	    // 2MK1: As of V5 of PDBx/mmCIF, saccharides seem to be represented as monomers,
	    //       instead of polysaccharides, so none of these tests returns true anymore.
	    List<String> pdbIds = Arrays.asList("2ONX","1JLP","5X6H","5L2G","2MK1");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new ContainsDSaccharideChain());    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertFalse(results.contains("1JLP"));
	    assertFalse(results.contains("5X6H"));
	    assertFalse(results.contains("5L2G"));
	    assertFalse(results.contains("2MK1"));
	}
	
	@Test
	public void test2() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new ContainsDSaccharideChain());    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX.A"));
	    assertFalse(results.contains("1JLP.A"));
	    assertFalse(results.contains("5X6H.B"));
	    assertFalse(results.contains("5L2G.A"));
	    assertFalse(results.contains("5L2G.B"));
	    assertFalse(results.contains("2MK1.A"));
	}
	
}
