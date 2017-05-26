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

public class ResolutionFilterTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ResolutionFilterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 5JT8: 2.1 A x-ray resolution
	    // 5VHT: 2.0 A x-ray resolution
	    // 5LO2: n/a 
	    // 5KHE: n/a (35 A EM resolution)
	    List<String> pdbIds = Arrays.asList("5JT8","5VHT","5LO2","5KHE");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new Resolution(2.099, 2.101));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("5JT8"));
	    assertFalse(results.contains("5VHT"));
	    assertFalse(results.contains("5LO2"));
	    assertFalse(results.contains("5KHE"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new Resolution(1.99, 2.01));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("5JT8"));
	    assertTrue(results.contains("5VHT"));
	    assertFalse(results.contains("5LO2"));
	    assertFalse(results.contains("5KHE"));
	}
	
	@Test
	/**
	 * The resolution is the x-ray resolution. Therefore, it should match EM resolution (5KHE)
	 */
	public void test3() {
	    pdb = pdb.filter(new Resolution(34.99, 35.01));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("5JT8"));
	    assertFalse(results.contains("5VHT"));
	    assertFalse(results.contains("5LO2"));
	    assertFalse(results.contains("5KHE"));
	}
	
}
