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

public class SecondaryStructureTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(SecondaryStructureTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 1AIE: all alpha protein 20 alpha out of 31 = 0.645 helical
	    // 1E0N: all beta protein, NMR structure with 10 models, 13 beta out of 27 = 0.481 sheet
	    // 1EM7: alpha + beta, 14 alpha + 23 beta out of 56 = 0.25 helical and 0.411 sheet
	    // 2C7M: 2 chains, alpha + beta (DSSP in MMTF doesn't match DSSP on RCSB PDB website)
	    List<String> pdbIds = Arrays.asList("1AIE","1E0N","1EM7","2C7M");
	    pdb = MmtfReader.downloadReducedMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new SecondaryStructure(0.64, 0.65, 0.0, 0.0, 0.35, 0.36));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1AIE"));
	    assertFalse(results.contains("1E0N"));
	    assertFalse(results.contains("1EM7"));
	    assertFalse(results.contains("2C7M"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new SecondaryStructure(0.0, 0.0, 0.48, 0.49, 0.51, 0.52));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1E0N"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new SecondaryStructure(0.24, 0.26, 0.41, 0.42, 0.33, 0.34));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1EM7"));
	}
	
	@Test
	public void test4() {
		boolean exclusive = true;
	    pdb = pdb.filter(new SecondaryStructure(0.70, 0.80, 0.00, 0.20, 0.20, 0.30, exclusive));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2C7M"));
	}
	
	@Test
	public void test5() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new SecondaryStructure(0.25, 0.75, 0.00, 0.40, 0.25, 0.50));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("2C7M.A"));
	    assertTrue(results.contains("2C7M.B"));
	}
	
	@Test
	public void test6() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new SecondaryStructure(0.70, 0.75, 0.00, 0.40, 0.25, 0.50));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("2C7M.A"));
	    assertFalse(results.contains("2C7M.B"));
	}
}
