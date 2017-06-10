package edu.sdsc.mmtf.spark.mappers;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

public class StructureToPolymerChainsTest {
	private JavaSparkContext sc;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(StructureToPolymerChainsTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test() {
	    List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);

	    // 1STP: 1 L-protein chain:
	    // 4HHB: 4 polymer chains
	    // 1JLP: 1 L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: 1 L-protein and 1 DNA chain
	    // 5L2G: 2 DNA chain
	    // 2MK1: 1 D-saccharide
	    // --------------------
	    /// tot: 11 chains
	    
	    JavaPairRDD<String, StructureDataInterface> polymers = pdb.flatMapToPair(new StructureToPolymerChains());
        assertEquals(11, polymers.count());
	}
}
