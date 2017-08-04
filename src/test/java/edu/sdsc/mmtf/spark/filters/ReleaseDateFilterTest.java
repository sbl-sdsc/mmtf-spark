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

public class ReleaseDateFilterTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReleaseDateFilterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 1O6Y: released on 2003-01-30
	    // 4MYA: released on 2014-01-01
	    // 3VCO: released on 2013-03-06
	    // 5N0Y: released on 2017-05-24
	    List<String> pdbIds = Arrays.asList("1O6Y","4MYA","3VCO","5N0Y");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new ReleaseDate("2000-01-01", "2010-01-01"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1O6Y"));
	    assertFalse(results.contains("4MYA"));
	    assertFalse(results.contains("3VCO"));
	    assertFalse(results.contains("5N0Y"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new ReleaseDate("2010-01-01", "2020-01-01"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1O6Y"));
	    assertTrue(results.contains("4MYA"));
	    assertTrue(results.contains("3VCO"));
	    assertTrue(results.contains("5N0Y"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new ReleaseDate("2013-03-06", "2013-03-06"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1O6Y"));
	    assertFalse(results.contains("4MYA"));
	    assertTrue(results.contains("3VCO"));
	    assertFalse(results.contains("5N0Y"));
	}
	
	@Test
	public void test4() {
	    pdb = pdb.filter(new ReleaseDate("2017-05-24", "2017-05-24"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1O6Y"));
	    assertFalse(results.contains("4MYA"));
	    assertFalse(results.contains("3VCO"));
	    assertTrue(results.contains("5N0Y"));
	}
}
