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
	    
	    // 2ONX: 1.52 A x-ray resolution
	    // 2OLX: 1.42 A x-ray resolution
	    // 3REC: n/a NMR structure
	    // 1LU3: 16.8 A EM resolution
	    List<String> pdbIds = Arrays.asList("2ONX","2OLX","3REC","1LU3");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new Resolution(1.51, 1.53));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("2ONX"));
	    assertFalse(results.contains("2OLX"));
	    assertFalse(results.contains("3REC"));
	    assertFalse(results.contains("1LU3"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new Resolution(1.41, 1.43));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertTrue(results.contains("2OLX"));
	    assertFalse(results.contains("3REC"));
	    assertFalse(results.contains("1LU3"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new Resolution(16.7, 16.9));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("2ONX"));
	    assertFalse(results.contains("2OLX"));
	    assertFalse(results.contains("3REC"));
	    assertTrue(results.contains("1LU3"));
	}
}
