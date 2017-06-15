package edu.sdsc.mmtf.spark.io;

import java.io.File;
import java.io.IOException;
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

public class MmtfWriterTest {
	private JavaSparkContext sc;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MmtfWriterTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
    }
    
    @After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void test1() throws IOException {
	    List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	  
	    File tempFile = folder.getRoot();
		MmtfWriter.writeMmtfFiles(tempFile.toString(), sc, pdb);
	}
}
