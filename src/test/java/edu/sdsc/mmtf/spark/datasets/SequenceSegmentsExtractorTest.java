package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class SequenceSegmentsExtractorTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(SequenceSegmentsExtractorTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);

	    List<String> pdbIds = Arrays.asList("1STP");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
		Dataset<Row> seq = SequenceSegmentsExtractor.getDataset(pdb,25);
		assertEquals("DPSKDSKAQVSAAEAGITGTWYNQL",seq.head().get(1));
	}
}
