package edu.sdsc.mmtf.spark.io;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class MmtfImporterTest {
	private JavaSparkContext sc;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MmtfImporterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
    }
    
    @After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
		Path p = Paths.get("./src/main/resources/files/");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfImporter.importPdbFiles(p.toString(), sc);
	    
	    assertTrue(pdb.count() == 2);
	}
	@Test
	public void test2() throws IOException {
		Path p = Paths.get("./src/main/resources/files/");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfImporter.importMmcifFiles(p.toString(), sc);
	    
	    assertTrue(pdb.count() == 1);
	}
	
	@Test
	public void test3() throws IOException {
		Path p = Paths.get("./src/main/resources/files/test");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfImporter.importPdbFiles(p.toString(), sc);
	    assertTrue(pdb.count() == 1);
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    assertTrue(pdb.count() == 8);
	}

	@Test
	public void test4() throws IOException {
		Path p = Paths.get("./src/main/resources/files/test");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfImporter.importMmcifFiles(p.toString(), sc);
	    assertTrue(pdb.count() == 1);
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    assertTrue(pdb.count() == 8);
	}
	
	@Test
	public void test5() throws IOException {
//	    List<String> pdbIds = Arrays.asList("1CBS");
//	    JavaPairRDD<String, StructureDataInterface> structures = MmtfImporter.downloadPdbRedo(pdbIds, sc);
	    String path = "/Users/peter/Downloads/";
	    JavaPairRDD<String, StructureDataInterface> structures = MmtfImporter.importMmcifFiles(path, sc);
	    
	    assertEquals(1, structures.count());
	}
}
