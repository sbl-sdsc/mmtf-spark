package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import edu.sdsc.mmtf.spark.datasets.DrugBankDataset;

/**
 * @author Peter Rose
 * @since 0.2.0
 */
public class DrugBankDatasetTest {

	@Test
	public void test() throws IOException {
		SparkSession spark = SparkSession
				.builder()
				.master("local[*]")
				.appName(DrugBankDatasetTest.class.getSimpleName())
				.getOrCreate();
		
		Dataset<Row> ds = DrugBankDataset.getOpenDrugLinks();
		assertTrue(ds.count() > 10000);
		assertEquals("DrugBankID", ds.columns()[0]);
		
		spark.close();
	}
}
