package edu.sdsc.mmtf.spark.rcsbfilters;

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
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class CustomReportQueryTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportQueryTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    List<String> pdbIds = Arrays.asList("5JDE","5CU4","5L6W","5UFU","5IHB");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	/**
	 * This test runs a chain level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test1() throws IOException {
		String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
		pdb = pdb.filter(new CustomReportQuery(whereClause, "ecNo","source"));
		List<String> matches = pdb.keys().collect();
		
		assertTrue(matches.contains("5JDE"));
		assertTrue(matches.contains("5CU4"));
		assertTrue(matches.contains("5L6W"));
		assertFalse(matches.contains("5UFU"));
		assertFalse(matches.contains("5IHB"));
	}
	
	@Test
	/**
	 *  This test runs a chain level query and compares chain level results
	 * @throws IOException
	 */
	public void test2() throws IOException {
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
		String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
		pdb = pdb.filter(new CustomReportQuery(whereClause, "ecNo","source"));
		List<String> matches = pdb.keys().collect();
		
		assertTrue(matches.contains("5JDE.A"));
		assertTrue(matches.contains("5JDE.B"));
		assertTrue(matches.contains("5CU4.A"));
		assertTrue(matches.contains("5L6W.L"));  // this chain is EC 2.7.11.1
		assertFalse(matches.contains("5L6W.C")); // this chain in not EC 2.7.11.1
		assertFalse(matches.contains("5UFU.A"));
		assertFalse(matches.contains("5UFU.B"));
		assertFalse(matches.contains("5UFU.C"));
		assertFalse(matches.contains("5IHB.A"));
		assertFalse(matches.contains("5IHB.B"));
		assertFalse(matches.contains("5IHB.C"));
		assertFalse(matches.contains("5IHB.D"));
	}
}
