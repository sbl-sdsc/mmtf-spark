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

public class RcsbAdvancedSearchTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;

	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(RcsbAdvancedSearchTest.class.getSimpleName());
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
		
		pdb = pdb.filter(new RcsbAdvancedSearch(query));
		List<String> matches = pdb.keys().collect();

		assertTrue(matches.contains("1PEN"));
		assertTrue(matches.contains("1OCZ"));
		assertFalse(matches.contains("2ONX"));
	}
	
	@Test
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
		
		pdb = pdb.filter(new RcsbAdvancedSearch(query));
		List<String> matches = pdb.keys().collect();

		assertFalse(matches.contains("1PEN"));
		assertFalse(matches.contains("1OCZ"));
		assertTrue(matches.contains("2ONX"));
	}
}
