package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
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
	 * @throws IOException
	 */
	@Test
	public void test1() throws IOException {
	    String sql = "SELECT * FROM sifts.pdb_chain_uniprot LIMIT 10";		
		Dataset<Row> ds = PdbjMineDataset.getDataset(sql);
			
		long count = ds.filter("structureChainId = '101M.A'").count();
		assertEquals(1, count);
	}	
}
