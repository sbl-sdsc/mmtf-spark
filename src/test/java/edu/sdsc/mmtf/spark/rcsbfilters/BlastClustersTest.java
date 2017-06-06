package edu.sdsc.mmtf.spark.rcsbfilters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.StructureException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClustersTest;

public class BlastClustersTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(BlastClustersTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
	    List<String> pdbIds = Arrays.asList("1O06","2ONX");
	    pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	/**
	 * This test runs a pdb level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test1() throws IOException, StructureException {
		pdb = pdb.filter(new BlastClusters(40));
		List<String> matches = pdb.keys().collect();
		
		assertTrue(matches.contains("1O06"));
		assertFalse(matches.contains("1O06.A"));
		assertFalse(matches.contains("2ONX"));
	}
	
	@Test
	/**
	 * This test runs a chain level query and compares the results at the PDB entry level
	 * @throws IOException
	 */
	public void test2() throws IOException, StructureException {
		pdb = pdb.filter(new BlastClusters(40));
		pdb = pdb.flatMapToPair(new StructureToPolymerChains());
		List<String> matches = pdb.keys().collect();
		
		assertFalse(matches.contains("1O06"));
		assertTrue(matches.contains("1O06.A"));
		assertFalse(matches.contains("2ONX.A"));
	}
}
