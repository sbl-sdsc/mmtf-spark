package edu.sdsc.mmtf.spark.filters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.apps.Demo1b;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class RcsbWebserviceFilterTest {

	@Test
	public void test1() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
		List<String> pdbIds = Arrays.asList("5JDE","5CU4","5L6W","5UFU","5IHB");
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    
	    // this test runs a chain level query and compares the results at the PDB entry level
		String whereClause = "WHERE ecNo='2.7.11.1'";
		pdb = pdb.filter(new RcsbWebServiceFilter(whereClause, "ecNo"));
		List<String> matches = pdb.keys().collect();
		sc.close();
		
		assertTrue(matches.contains("5JDE"));
		assertTrue(matches.contains("5CU4"));
		assertTrue(matches.contains("5L6W"));
		assertTrue(matches.contains("5UFU"));
		assertFalse(matches.contains("5IHB"));
	}
	
	@Test
	public void test2() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
		List<String> pdbIds = Arrays.asList("5JDE","5CU4","5L6W","5UFU","5IHB");
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    
	    // this test runs a chain level query and compares the results at the PDB entry level
		String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
		pdb = pdb.filter(new RcsbWebServiceFilter(whereClause, "ecNo","source"));
		List<String> matches = pdb.keys().collect();
		sc.close();
		
		assertTrue(matches.contains("5JDE"));
		assertTrue(matches.contains("5CU4"));
		assertTrue(matches.contains("5L6W"));
		assertFalse(matches.contains("5UFU"));
		assertFalse(matches.contains("5IHB"));
	}
	
	@Test
	public void test3() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
		List<String> pdbIds = Arrays.asList("5JDE","5CU4","5L6W","5UFU","5IHB");
	    // read PDB in MMTF format
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    
	    // this test runs a chain level query and compares chain level results
		String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
		pdb = pdb.filter(new RcsbWebServiceFilter(whereClause, "ecNo","source"));
		List<String> matches = pdb.keys().collect();
		sc.close();
		
		assertTrue(matches.contains("5JDE.A"));
		assertTrue(matches.contains("5JDE.B"));
		assertTrue(matches.contains("5CU4.A"));
		assertTrue(matches.contains("5L6W.L"));  // this chain is EC 2.7.11.1
		assertFalse(matches.contains("5L6W.C")); // this chain in not EC 2.7.11.1
		assertFalse(matches.contains("5UFU.A"));
		assertFalse(matches.contains("5UFU.B"));
		assertFalse(matches.contains("5UFU.C"));
		assertFalse(matches.contains("5IHB.A"));
		assertFalse(matches.contains("5IHB.B"));
		assertFalse(matches.contains("5IHB.C"));
		assertFalse(matches.contains("5IHB.D"));
	}

}
