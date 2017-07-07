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

public class DepositionDateFilterTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(DepositionDateFilterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 4MYA: deposited on 2013-09-27
	    // 1O6Y: deposited on 2002-10-21
	    // 3VCO: deposited on 2012-01-04
	    // 5N0Y: deposited on 2017-02-03
	    List<String> pdbIds = Arrays.asList("4MYA","1O6Y","3VCO","5N0Y");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new DepositionDate("2000-01-01", "2010-01-01"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1O6Y"));
	    assertFalse(results.contains("4MYA"));
	    assertFalse(results.contains("3VCO"));
	    assertFalse(results.contains("5N0Y"));
	}
	
	@Test
	public void test2() {
	    pdb = pdb.filter(new DepositionDate("2010-01-01", "2015-01-01"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1O6Y"));
	    assertTrue(results.contains("4MYA"));
	    assertTrue(results.contains("3VCO"));
	    assertFalse(results.contains("5N0Y"));
	}
	
	@Test
	public void test3() {
	    pdb = pdb.filter(new DepositionDate("2017-02-03", "2017-02-03"));    
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1O6Y"));
	    assertFalse(results.contains("4MYA"));
	    assertFalse(results.contains("3VCO"));
	    assertTrue(results.contains("5N0Y"));
	}
}
