package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.*;

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

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsAlternativeLocationsTest;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class SecondaryStructureExtractorTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ContainsAlternativeLocationsTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);

	    List<String> pdbIds = Arrays.asList("1STP","4HHB");
	    pdb = MmtfReader.downloadReducedMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() {
		pdb = pdb.filter(new ContainsLProteinChain());
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    pdb = pdb.filter(new ContainsLProteinChain());
		Dataset<Row> secStruct = SecondaryStructureExtractor.getDataset(pdb);
        assertEquals(5, secStruct.count());
	}
}
