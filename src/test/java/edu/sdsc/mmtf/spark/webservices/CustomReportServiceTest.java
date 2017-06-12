package edu.sdsc.mmtf.spark.webservices;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import edu.sdsc.mmtf.spark.datasets.CustomReportService;

/**
 * 
 * @author Peter Rose
 *
 */
public class CustomReportServiceTest {

	@Test
	public void test1() throws IOException {
		Dataset<Row> ds = CustomReportService.getDataset("pmc","pubmedId","depositionDate");
        
        assertEquals("StructType(StructField(structureId,StringType,true), StructField(pmc,StringType,true), StructField(pubmedId,IntegerType,true), StructField(depositionDate,TimestampType,true))",ds.schema().toString());
        assertTrue(ds.count() > 130101);
        
        ds.sparkSession().close();
	}
	
	@Test
	public void test2() throws IOException {
		Dataset<Row> ds = CustomReportService.getDataset("ecNo");
        
        assertEquals("StructType(StructField(structureChainId,StringType,true), StructField(structureId,StringType,true), StructField(chainId,StringType,true), StructField(ecNo,StringType,true))",ds.schema().toString());
        assertTrue(ds.count() > 130101);
        
        ds.sparkSession().close();
	}
}
