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
        assertTrue(ds.count() > 130101);
        
        ds.sparkSession().close();
	}

}
