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

public class AdvancedQueryTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;

	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(AdvancedQueryTest.class.getSimpleName());
		sc = new JavaSparkContext(conf);

		// 1PEN wildtype query 100 matches: 1PEN:1
		// 1OCZ two entities wildtype query 100 matches: 1OCZ:1, 1OCZ:2
		// 2ONX structure result for author query
		// 5L6W two chains: chain L is EC 2.7.11.1, chain chain C is not EC 2.7.11.1
		// 5KHU many chains, chain Q is EC 2.7.11.1
		// 1F3M entity 1: chains A,B, entity 2: chains B,C, all chains are EC 2.7.11.1
		List<String> pdbIds = Arrays.asList("1PEN","1OCZ","2ONX","5L6W","5KHU","1F3M");
		pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

//	@Test
	/**
	 * This test runs a chain level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test1() throws IOException {
		String query = 
				"<orgPdbQuery>" +
	                "<queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>" +
				    "<includeExprTag>Y</includeExprTag>" +
	                "<percentSeqAlignment>100</percentSeqAlignment>" +
				"</orgPdbQuery>";
		
		pdb = pdb.filter(new AdvancedQuery(query));
		List<String> matches = pdb.keys().collect();

		assertTrue(matches.contains("1PEN"));
		assertTrue(matches.contains("1OCZ"));
		assertFalse(matches.contains("2ONX"));
		assertFalse(matches.contains("5L6W"));
	}
	
//	@Test
	/**
	 * This test runs a chain level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test2() throws IOException {
		String query = 
				"<orgPdbQuery>" +
				    "<queryType>org.pdb.query.simple.AdvancedAuthorQuery</queryType>" +
				    "<searchType>All Authors</searchType><audit_author.name>Eisenberg</audit_author.name>" +
				    "<exactMatch>false</exactMatch>" +
				"</orgPdbQuery>";
		
		pdb = pdb.filter(new AdvancedQuery(query));
		List<String> matches = pdb.keys().collect();

		assertFalse(matches.contains("1PEN"));
		assertFalse(matches.contains("1OCZ"));
		assertTrue(matches.contains("2ONX"));
		assertFalse(matches.contains("5L6W"));
	}
	
//	@Test
	/**
	 * This test runs a chain level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test3() throws IOException {
		String query = 
				"<orgPdbQuery>" +
				    "<queryType>org.pdb.query.simple.EnzymeClassificationQuery</queryType>" +
				    "<Enzyme_Classification>2.7.11.1</Enzyme_Classification>" +
				"</orgPdbQuery>";
		
		pdb = pdb.filter(new AdvancedQuery(query));
		List<String> matches = pdb.keys().collect();

		assertFalse(matches.contains("1PEN"));
		assertFalse(matches.contains("1OCZ"));
		assertFalse(matches.contains("2ONX"));
		assertTrue(matches.contains("5L6W"));
		assertTrue(matches.contains("5KHU"));
	}
	
//	@Test
	/**
	 * This test runs a chain level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test4() throws IOException {
		String query = 
				"<orgPdbQuery>" +
				    "<queryType>org.pdb.query.simple.EnzymeClassificationQuery</queryType>" +
				    "<Enzyme_Classification>2.7.11.1</Enzyme_Classification>" +
				"</orgPdbQuery>";
		
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
		pdb = pdb.filter(new AdvancedQuery(query));
		List<String> matches = pdb.keys().collect();

		assertFalse(matches.contains("1PEN.A"));
		assertFalse(matches.contains("1OCZ.A"));
		assertFalse(matches.contains("2ONX.A"));
		assertTrue(matches.contains("5L6W.L")); // only this chain is EC 2.7.11.1
		assertFalse(matches.contains("5L6W.C")); 
		assertFalse(matches.contains("5KHU.A"));
		assertFalse(matches.contains("5KHU.B"));
		assertTrue(matches.contains("5KHU.Q")); // only this chain is EC 2.7.11.1
		assertTrue(matches.contains("1F3M.A")); // 1F3M all chains are EC 2.7.11.1
		assertTrue(matches.contains("1F3M.B")); 
		assertTrue(matches.contains("1F3M.C")); 
		assertTrue(matches.contains("1F3M.D")); 
	}
}
