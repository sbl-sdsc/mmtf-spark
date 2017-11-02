package edu.sdsc.mmtf.spark.mappers;

import static org.junit.Assert.assertEquals;

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

public class StructureToProteinDimersTest {
	private JavaSparkContext sc;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(StructureToProteinDimersTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
	    List<String> pdbIds = Arrays.asList("1I1G"); // D4
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	   
		
		pdb = pdb.flatMapToPair(new StructureToBioassembly()).flatMapToPair(new StructureToProteinDimers(8, 20, false, true));
	    long count = pdb.count();
	    assertEquals(4, count);
	}
	
	@Test
	public void test2() throws IOException {
	    List<String> pdbIds = Arrays.asList("5NV3"); // D4
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	   
		
		pdb = pdb.flatMapToPair(new StructureToBioassembly()).flatMapToPair(new StructureToProteinDimers(8, 20, false, true));
	    long count = pdb.count();
	    assertEquals(12, count);
	}
	
	@Test
	public void test3() throws IOException {
	    List<String> pdbIds = Arrays.asList("4GIS");//D4
	    /*
	     * A3-A2
	     * A4-A1
	     * B5-A1
	     * B6-A2
	     * B6-B5
	     * B7-A3
	     * B7-A4
	     * B8-A4
	     * B8-B7
	     */
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	   
		
		pdb = pdb.flatMapToPair(new StructureToBioassembly()).flatMapToPair(new StructureToProteinDimers(8, 20, false, true));
	    long count = pdb.count();
	    assertEquals(9, count);
	}
	    
	@Test
	public void test4() throws IOException {
	    List<String> pdbIds = Arrays.asList("1BZ5");//D5
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	    /*
	     * C5-B4
	     * C6-B3
	     * D7-A2
	     * D8-A1
	     * E10-E9
	     */
		
		pdb = pdb.flatMapToPair(new StructureToBioassembly()).flatMapToPair(new StructureToProteinDimers(9, 10, false, true));
	    long count = pdb.count();
	    assertEquals(5, count);
	}
}
