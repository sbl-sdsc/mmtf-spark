package edu.sdsc.mmtf.spark.webservices;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.sdsc.mmtf.spark.datasets.CustomReportService;
import edu.sdsc.mmtf.spark.rcsbfilters.AdvancedQueryTest;

/**
 * 
 * @author Peter Rose
 *
 */
public class CustomReportServiceTest {
	private JavaSparkContext sc;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(AdvancedQueryTest.class.getSimpleName());
		sc = new JavaSparkContext(conf);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
		Dataset<Row> ds = CustomReportService.getDataset("pmc","pubmedId","depositionDate");
        
        assertEquals("StructType(StructField(structureId,StringType,true), StructField(pmc,StringType,true), StructField(pubmedId,IntegerType,true), StructField(depositionDate,TimestampType,true))",ds.schema().toString());
        assertTrue(ds.count() > 130101);
	}
	
	@Test
	public void test2() throws IOException {
		Dataset<Row> ds = CustomReportService.getDataset("ecNo");
        
        assertEquals("StructType(StructField(structureChainId,StringType,true), StructField(structureId,StringType,true), StructField(chainId,StringType,true), StructField(ecNo,StringType,true))",ds.schema().toString());
        assertTrue(ds.count() > 130101);
	}
}
