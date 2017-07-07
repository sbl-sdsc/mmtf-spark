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

import edu.sdsc.mmtf.spark.filters.ContainsAlternativeLocationsTest;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class SecondaryStructureExtractorTest2 {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsAlternativeLocationsTest.class.getSimpleName());
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
		Dataset<Row> secStruct = SecondaryStructureExtractor.getDataset(pdb);
		String dsspQ8 = (String) secStruct.first().get(secStruct.first().fieldIndex("dsspQ8Code"));
		String dsspQ3 = (String) secStruct.first().get(secStruct.first().fieldIndex("dsspQ3Code"));
        assertEquals(38, dsspQ8.split("X", -1).length - 1);//"X" appears 38 times.
        assertEquals(24, dsspQ8.split(" ", -1).length - 1);//" " appears 24 times in dsspQ8
        assertEquals(44, dsspQ3.split(" ", -1).length - 1);//" " appears 44 times in dsspQ3
	}
}
