package edu.sdsc.mmtf.spark.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.demos.Demo1b;
import edu.sdsc.mmtf.spark.incubator.ReducedEncoderNew;
import scala.Tuple2;

public class MmtfWriterTest {

//	@Test
	public void test1() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    
	    Path tempFile = Files.createTempFile(null,"");
		MmtfWriter.writeMmtfFiles(tempFile.toString(), sc, pdb);
		
		sc.close();
	}
	
//	@Test
	public void test2() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    
//	    Path tempFile = Files.createTempDirectory(null);
	    // TODO create a temporary dir, that cannot be overwritten by writeMmtfFiles (check file attributes)
	    Path tempFile = new File("/Users/peter/work/mmtf-spark/").toPath();
	    System.out.println("fn: " + tempFile.toString());
	    System.out.println("fn: " + tempFile.getFileName().toString());
		MmtfWriter.writeMmtfFiles(tempFile.toString(), sc, pdb);
		
		sc.close();
	}

}
