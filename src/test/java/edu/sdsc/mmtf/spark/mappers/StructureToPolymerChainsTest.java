package edu.sdsc.mmtf.spark.mappers;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.apps.Demo1b;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

public class StructureToPolymerChainsTest {
	// should use mmtf files from test project here ...
	private static String path = "/Users/peter/MMTF_Files/full";

	@Test
	public void test() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    Set<String> pdbIds = new HashSet<String>(Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1"));
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(path, pdbIds, sc);

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
	    
	    sc.close();
	}

}
