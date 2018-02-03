package edu.sdsc.mmtf.spark.datasets;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PdbjMineDatasetTest {
	private SparkSession spark;
	
	@Before
	public void setUp() throws Exception {
		spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(PdbjMineDatasetTest.class.getSimpleName())
				.getOrCreate();
	}

	@After
	public void tearDown() throws Exception {
		spark.close();
	}

	/**
	 * This test runs a very simple SQL query, checking the number of entries in the PDB
	 * @throws IOException
	 */
	@Test
	public void test1() throws IOException {
		String sql = "select distinct entity.pdbid from entity join entity_src_gen on entity_src_gen.pdbid=entity.pdbid where pdbx_ec='2.7.11.1' and pdbx_gene_src_scientific_name='Homo sapiens'";
		
		Dataset<Row> ds = PdbjMineDataset.getDataset(sql);
		
		// rename pdbId field and convert to upper case for compatibility with this projects
		ds = ds.withColumnRenamed("pdbId", "structureId");
		ds = ds.withColumn("structureId", upper(col("structureId")));
			
		List<String> matches = ds.select("structureId").as(Encoders.STRING()).collectAsList();
		assertTrue(matches.contains("5JDE"));
		assertTrue(matches.contains("5CU4"));
		assertTrue(matches.contains("5L6W"));
		assertFalse(matches.contains("5UFU"));
		assertFalse(matches.contains("5IHB"));
	}	
	

	/**
	 *  This test runs a chain level SQL query and compares chain level results
	 * @throws IOException
	 */
	@Test
	public void test2() throws IOException {
		String sql = "select distinct concat(entity_poly.pdbid, '.', unnest(string_to_array(entity_poly.pdbx_strand_id, ','))) as \"structureChainId\" from entity_poly join entity_src_gen on entity_src_gen.pdbid=entity_poly.pdbid and entity_poly.entity_id=entity_poly.entity_id join entity on entity.pdbid=entity_poly.pdbid and entity.id=entity_poly.entity_id where pdbx_ec='2.7.11.1' and pdbx_gene_src_scientific_name='Homo sapiens'";
		Dataset<Row> ds = PdbjMineDataset.getDataset(sql);
		// rename pdbId field and convert to upper case for compatibility with this projects
		ds.show(100);
//		ds = ds.withColumnRenamed("pdbId", "structureId");
//		ds = ds.withColumn("structureId", upper(col("structureId")));
//		List<String> matches = ds.select("structureId").as(Encoders.STRING()).collectAsList();
//		
//
//		assertTrue(matches.contains("5JDE.A"));
//		assertTrue(matches.contains("5JDE.B"));
//		assertTrue(matches.contains("5CU4.A"));
//		assertTrue(matches.contains("5L6W.L"));  // this chain is EC 2.7.11.1
//		assertFalse(matches.contains("5L6W.C")); // this chain in not EC 2.7.11.1
//		assertFalse(matches.contains("5UFU.A"));
//		assertFalse(matches.contains("5UFU.B"));
//		assertFalse(matches.contains("5UFU.C"));
//		assertFalse(matches.contains("5IHB.A"));
//		assertFalse(matches.contains("5IHB.B"));
//		assertFalse(matches.contains("5IHB.C"));
//		assertFalse(matches.contains("5IHB.D"));
	}
}
