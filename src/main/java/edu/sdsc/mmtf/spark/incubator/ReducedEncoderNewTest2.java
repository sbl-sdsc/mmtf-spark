package edu.sdsc.mmtf.spark.incubator;


import static org.junit.Assert.*;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.ReducedEncoder;

import edu.sdsc.mmtf.spark.demos.Demo1b;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;
import scala.Tuple2;

public class ReducedEncoderNewTest2 {

	@Test
	public void test() {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReducedEncoderNewTest2.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    // 1PLX: small NMR with 80 models
	    List<String> pdbIds = Arrays.asList("4CK4","3ZYB","1R9V","1LPV","1PLX");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache();	    
	 
	    pdb = pdb.mapValues(v -> ReducedEncoderNew.getReduced(v)).cache();
//	    pdb = pdb.mapValues(v -> ReducedEncoder.getReduced(v)).cache();
	    Path tempFile = new File("/Users/peter/work/mmtf-spark/").toPath();
		MmtfWriter.writeMmtfFiles(tempFile.toString(), sc, pdb);
		
		sc.close();
	}

}
