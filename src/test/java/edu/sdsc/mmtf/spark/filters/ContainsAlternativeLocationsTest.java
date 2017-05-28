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

public class ContainsAlternativeLocationsTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsAlternativeLocationsTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    // 4QXX: has alternative location ids
	    // 2ONX: has no alternative location ids
	    List<String> pdbIds = Arrays.asList("4QXX","2ONX");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
	    pdb = pdb.filter(new ContainsAlternativeLocations());    
	    List<String> results = pdb.keys().collect();

	    assertTrue(results.contains("4QXX"));
	    assertFalse(results.contains("2ONX"));
	}
}
