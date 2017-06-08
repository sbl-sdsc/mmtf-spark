package edu.sdsc.mmtf.spark.mappers;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.demos.Demo1b;
import edu.sdsc.mmtf.spark.io.MmtfReader;

public class StructureToBioJavaTest {

	@Test
	public void test() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo1b.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache();

		// 1STP: 1 L-protein chain:
		// 4HHB: 4 polymer chains
		// 1JLP: 1 L-protein chains with non-polymer capping group (NH2)
		// 5X6H: 1 L-protein and 1 DNA chain
		// 5L2G: 2 DNA chains
		// 2MK1: 1 D-saccharide
		// --------------------
		// tot : 11 polymer chains

		JavaDoubleRDD chainCounts = pdb
				.mapToPair(new StructureToBioJava())
				.values()
				.mapToDouble(v -> v.getPolyChains().size());

		assertEquals(11, Math.round(chainCounts.sum()));

		// extract polymer chains and count chains again
		chainCounts = pdb
				.flatMapToPair(new StructureToPolymerChains())
				.mapToPair(new StructureToBioJava())
				.values()
				.mapToDouble(v -> v.getChains().size());
				
		assertEquals(11, Math.round(chainCounts.sum()));

		sc.close();
	}

}
