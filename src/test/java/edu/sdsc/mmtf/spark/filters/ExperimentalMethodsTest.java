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

public class ExperimentalMethodsTest {
	JavaSparkContext sc;
	JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ExperimentalMethodsTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    List<String> pdbIds = Arrays.asList("1STP","5VLN","5VAI","5JXV","5K7N","3PDM","5MNX","5I1R","5MON","5LCB","3J07");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}
	
	@Test
	public void test1() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))
				 .keys()
				 .collect();
 
	    assertTrue(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test1a() {
		 List<String> results = pdb
				 .flatMapToPair(new StructureToPolymerChains())
				 .filter(new ExperimentalMethods(ExperimentalMethods.X_RAY_DIFFRACTION))
				 .keys()
				 .collect();
 
	    assertTrue(results.contains("1STP.A"));
	    assertFalse(results.contains("5VLN.A"));
	}
	
	@Test
	public void test2() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.SOLUTION_NMR))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertTrue(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test3() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.ELECTRON_MICROSCOPY))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertTrue(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test4() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.SOLID_STATE_NMR))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertTrue(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test5() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.ELECTRON_CRYSTALLOGRAPHY))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertTrue(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test6() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.FIBER_DIFFRACTION))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertTrue(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test7() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.NEUTRON_DIFFRACTION))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertTrue(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test8() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.SOLUTION_SCATTERING, ExperimentalMethods.SOLUTION_NMR))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertTrue(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test9() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.SOLUTION_NMR, ExperimentalMethods.SOLUTION_SCATTERING))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertTrue(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertFalse(results.contains("3J07"));
	}
	
	@Test
	public void test10() {
		 List<String> results = pdb
				 .filter(new ExperimentalMethods(ExperimentalMethods.SOLID_STATE_NMR, ExperimentalMethods.ELECTRON_MICROSCOPY, ExperimentalMethods.SOLUTION_SCATTERING))
				 .keys()
				 .collect();
 
	    assertFalse(results.contains("1STP"));
	    assertFalse(results.contains("5VLN"));
	    assertFalse(results.contains("5VAI"));
	    assertFalse(results.contains("5JXV"));
	    assertFalse(results.contains("5K7N"));
	    assertFalse(results.contains("3PDM"));
	    assertFalse(results.contains("5MNX"));
	    assertFalse(results.contains("5I1R"));
	    assertFalse(results.contains("5MON"));
	    assertFalse(results.contains("5LCB"));
	    assertTrue(results.contains("3J07"));
	}
}
