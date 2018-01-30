package edu.sdsc.mmtf.spark.webfilters;

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
import edu.sdsc.mmtf.spark.webfilters.MineSearch;

public class MineSearchTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MineSearchTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    List<String> pdbIds = Arrays.asList("5JDE","5CU4","5L6W","5UFU","5IHB");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

// TODO	@Test
	/**
	 * This test runs a very simple SQL query, checking the number of entries in the RDB
	 * @throws IOException
	 */
	public void test1() throws IOException {
//		String sql = "select count(*) from brief_summary";
		
//		MineSearch search = new MineSearch(sql);
//		search.dataset.show();

//		assertTrue(((Integer)search.dataset.head().get(0)) > 100000);
	}	
	
	@Test
	/**
	 * This test runs a SQL query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test2() throws IOException {
		String sql = "select distinct entity.pdbid from entity join entity_src_gen on entity_src_gen.pdbid=entity.pdbid where pdbx_ec='2.7.11.1' and pdbx_gene_src_scientific_name='Homo sapiens'";
		
		pdb = pdb.filter(new MineSearch(sql));
		List<String> matches = pdb.keys().collect();
		
		assertTrue(matches.contains("5JDE"));
		assertTrue(matches.contains("5CU4"));
		assertTrue(matches.contains("5L6W"));
		assertFalse(matches.contains("5UFU"));
		assertFalse(matches.contains("5IHB"));
	}
	
	@Test
	/**
	 *  This test runs a chain level SQL query and compares chain level results
	 * @throws IOException
	 */
	public void test3() throws IOException {
		String sql = "select distinct concat(entity_poly.pdbid, '.', unnest(string_to_array(entity_poly.pdbx_strand_id, ','))) as \"structureChainId\" from entity_poly join entity_src_gen on entity_src_gen.pdbid=entity_poly.pdbid and entity_poly.entity_id=entity_poly.entity_id join entity on entity.pdbid=entity_poly.pdbid and entity.id=entity_poly.entity_id where pdbx_ec='2.7.11.1' and pdbx_gene_src_scientific_name='Homo sapiens'";
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    
		pdb = pdb.filter(new MineSearch(sql, "structureChainId", true));
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
