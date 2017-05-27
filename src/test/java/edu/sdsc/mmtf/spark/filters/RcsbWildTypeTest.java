package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

import edu.sdsc.mmtf.spark.io.MmtfReader;

public class RcsbWildTypeTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(RcsbWildTypeTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
		// 1PEN wildtype query 100 matches: 1PEN:1
		// 1OCZ two entities wildtype query 100 matches: 1OCZ:1, 1OCZ:2
		// 2ONX structure result for author query
		List<String> pdbIds = Arrays.asList("1PEN","1OCZ","2ONX");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
	    pdb = pdb.filter(new RcsbWildType(true, RcsbWildType.SEQUENCE_COVERAGE_100));    
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1PEN"));
	    assertTrue(results.contains("1OCZ"));
	    assertFalse(results.contains("2ONX"));
	}
}
