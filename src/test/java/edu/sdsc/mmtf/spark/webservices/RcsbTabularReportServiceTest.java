package edu.sdsc.mmtf.spark.webservices;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

/**
 * 
 * @author Peter Rose
 *
 */
public class RcsbTabularReportServiceTest {

	@Test
	public void test() throws IOException {
		RcsbTabularReportService downloader = new RcsbTabularReportService();

		Dataset<Row> ds = downloader.getDataset(Arrays.asList("pmc","pubmedId","depositionDate"));
        ds.printSchema();
        ds.show(5);
        
        assertEquals("StructType(StructField(structureId,StringType,true), StructField(pmc,StringType,true), StructField(pubmedId,IntegerType,true), StructField(depositionDate,TimestampType,true))",ds.schema().toString());
        assertTrue(ds.count() > 130101);
        
        ds.sparkSession().close();
	}

}
