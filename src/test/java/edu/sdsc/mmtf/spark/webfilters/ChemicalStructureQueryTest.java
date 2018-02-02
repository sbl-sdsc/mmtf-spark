package edu.sdsc.mmtf.spark.webfilters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import edu.sdsc.mmtf.spark.webfilters.ChemicalStructureQuery;

public class ChemicalStructureQueryTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ChemicalStructureQueryTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	    
		List<String> pdbIds = Arrays.asList("1HYA","2ONX","1F27","4QMC","2RTL");
	    pdb = MmtfReader.downloadReducedMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
	    pdb = pdb.filter(new ChemicalStructureQuery("CC(=O)NC1C(O)OC(CO)C(O)C1O"));
	    List<String> results = pdb.keys().collect();
	    
	    assertTrue(results.contains("1HYA"));
	    assertFalse(results.contains("2ONX"));
	}
	
	@Test
	public void test2() throws IOException {
	    pdb = pdb.filter(new ChemicalStructureQuery("OC(=O)CCCC[C@@H]1SC[C@@H]2NC(=O)N[C@H]12", ChemicalStructureQuery.EXACT, 0));
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1HYA"));
	    assertFalse(results.contains("2ONX"));
	    assertTrue(results.contains("1F27"));
	    assertFalse(results.contains("2RTL"));
	    assertFalse(results.contains("4QMC"));
	}
	
	@Test
	public void test3() throws IOException {
	    pdb = pdb.filter(new ChemicalStructureQuery("OC(=O)CCCC[C@@H]1SC[C@@H]2NC(=O)N[C@H]12", ChemicalStructureQuery.SUBSTRUCTURE, 0));
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1HYA"));
	    assertFalse(results.contains("2ONX"));
	    assertTrue(results.contains("1F27"));
	    assertFalse(results.contains("2RTL"));
	    assertTrue(results.contains("4QMC"));
	}

	@Test
	public void test4() throws IOException {
	    pdb = pdb.filter(new ChemicalStructureQuery("OC(=O)CCCC[C@@H]1SC[C@@H]2NC(=O)N[C@H]12", ChemicalStructureQuery.SIMILAR, 70));
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1HYA"));
	    assertFalse(results.contains("2ONX"));
	    assertTrue(results.contains("1F27"));
	    assertTrue(results.contains("2RTL"));
	    assertTrue(results.contains("4QMC"));
	}
	
	@Test
	public void test5() throws IOException {
	    pdb = pdb.filter(new ChemicalStructureQuery("OC(=O)CCCC[C@H]1[C@H]2NC(=O)N[C@H]2C[S@@]1=O", ChemicalStructureQuery.SUPERSTRUCTURE, 0));
	    List<String> results = pdb.keys().collect();
	    
	    assertFalse(results.contains("1HYA"));
	    assertFalse(results.contains("2ONX"));
	    assertTrue(results.contains("1F27"));
	    assertFalse(results.contains("2RTL"));
	    assertTrue(results.contains("4QMC"));
	}
}
