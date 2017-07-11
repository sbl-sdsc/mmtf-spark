package edu.sdsc.mmtf.spark.rcsbfilters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.PolymerCompositionTest;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.Pisces;

public class PiscesTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PolymerCompositionTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    // "4R4X.A" and "5X42.B" should pass filter
	    List<String> pdbIds = Arrays.asList("5X42","4R4X","2ONX","1JLP");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
	    pdb = pdb.filter(new Pisces(20, 2.0));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("5X42"));
	    assertTrue(results.contains("4R4X"));
	    assertFalse(results.contains("2ONX"));
	    assertFalse(results.contains("1JLP"));
	}
	@Test
	
	public void test2() throws IOException {
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
		pdb = pdb.filter(new Pisces(20, 2.0));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("5X42.B"));
	    assertTrue(results.contains("4R4X.A"));
	    assertFalse(results.contains("5X42.A"));
	    assertFalse(results.contains("2ONX.A"));
	    assertFalse(results.contains("1JLP.A"));
	}
}
