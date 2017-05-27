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
	    
	    // 2ONX: 0.202 rfree x-ray resolution
	    // 2OLX: 0.235 rfree x-ray resolution
	    // 3REC: n/a NMR structure
	    // 1LU3: n/a EM structure
	    List<String> pdbIds = Arrays.asList("2ONX","2OLX","3REC","1LU3");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new Rfree(0.201, 0.203));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("2ONX"));
	    assertFalse(results.contains("2OLX"));
	    assertFalse(results.contains("3REC"));
	    assertFalse(results.contains("5KHE"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new Rfree(0.234, 0.236));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertTrue(results.contains("2OLX"));
	    assertFalse(results.contains("3REC"));
	    assertFalse(results.contains("5KHE"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new Rfree(0.15, 0.2));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertFalse(results.contains("2OLX"));
	    assertFalse(results.contains("3REC"));
	    assertFalse(results.contains("5KHE"));
	}
}
