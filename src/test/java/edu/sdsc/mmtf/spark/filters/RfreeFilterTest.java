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

public class RfreeFilterTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(RfreeFilterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 5JT8: 0.217 rfree
	    // 5VHT: 0.240 rfree
	    // 5LO2: n/a for NMR structure
	    // 5KHE" n/a for EM structure
	    List<String> pdbIds = Arrays.asList("5JT8","5VHT","5LO2","5KHE");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new Rfree(0.216, 0.218));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("5JT8"));
	    assertFalse(results.contains("5VHT"));
	    assertFalse(results.contains("5LO2"));
	    assertFalse(results.contains("5KHE"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new Rfree(0.239, 0.241));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("5JT8"));
	    assertTrue(results.contains("5VHT"));
	    assertFalse(results.contains("5LO2"));
	    assertFalse(results.contains("5KHE"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new Rfree(0.15, 0.2));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("5JT8"));
	    assertFalse(results.contains("5VHT"));
	    assertFalse(results.contains("5LO2"));
	    assertFalse(results.contains("5KHE"));
	}
	
}
