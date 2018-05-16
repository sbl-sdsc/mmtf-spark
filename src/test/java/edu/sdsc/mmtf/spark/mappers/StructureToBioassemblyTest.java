package edu.sdsc.mmtf.spark.mappers;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

public class StructureToBioassemblyTest {
	private JavaSparkContext sc;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(StructureToBioassemblyTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}
	
//	@Test
	public void test1() {
		// 2HHB: asymmetric unit corresponds to biological assembly
		// see: http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/biological-assemblies
		List<String> pdbIds = Arrays.asList("2HHB");
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.downloadFullMmtfFiles(pdbIds, sc)
				.flatMapToPair(new StructureToBioassembly2());
		
		Map<String, StructureDataInterface> map = pdb.collectAsMap();
		
		assertEquals(1, map.size());
		assertEquals(1, map.get("2HHB-BioAssembly1").getNumModels());
		assertEquals(14, map.get("2HHB-BioAssembly1").getNumChains());
		assertEquals(801, map.get("2HHB-BioAssembly1").getNumGroups());
		assertEquals(4779, map.get("2HHB-BioAssembly1").getNumAtoms());
		assertEquals(4130, map.get("2HHB-BioAssembly1").getNumBonds());
	}

	@Test
	public void test2() {
		// 1OUT: asymmetric unit corresponds to 1/2 of biological assembly
		// see: http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/biological-assemblies
		List<String> pdbIds = Arrays.asList("1OUT");
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.downloadFullMmtfFiles(pdbIds, sc)
				.flatMapToPair(new StructureToBioassembly2());
				
	    Map<String, StructureDataInterface> map = pdb.collectAsMap();
		
		assertEquals(1, map.size()); // 1 bioassembly
		assertEquals(1, map.get("1OUT-BioAssembly1").getNumModels());
		assertEquals(12, map.get("1OUT-BioAssembly1").getNumChains());
		assertEquals(928, map.get("1OUT-BioAssembly1").getNumGroups());
		assertEquals(4950, map.get("1OUT-BioAssembly1").getNumAtoms());
		assertEquals(4174, map.get("1OUT-BioAssembly1").getNumBonds());
	}
	
	@Test
	public void test3() {
		// 1HV4: asymmetric unit corresponds to 2 biological assemblies
		// see: http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/biological-assemblies
		List<String> pdbIds = Arrays.asList("1HV4");
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader
				.downloadFullMmtfFiles(pdbIds, sc)
				.flatMapToPair(new StructureToBioassembly2());
		
		Map<String, StructureDataInterface> map = pdb.collectAsMap();
		
		assertEquals(2, map.size()); // 2 bioassemblies
		
		assertEquals(1, map.get("1HV4-BioAssembly1").getNumModels());
		assertEquals(8, map.get("1HV4-BioAssembly1").getNumChains());
		assertEquals(578, map.get("1HV4-BioAssembly1").getNumGroups());
		assertEquals(4644, map.get("1HV4-BioAssembly1").getNumAtoms());
		assertEquals(4210, map.get("1HV4-BioAssembly1").getNumBonds());
		
		assertEquals(1, map.get("1HV4-BioAssembly2").getNumModels());
		assertEquals(8, map.get("1HV4-BioAssembly2").getNumChains());
		assertEquals(578, map.get("1HV4-BioAssembly2").getNumGroups());
		assertEquals(4644, map.get("1HV4-BioAssembly2").getNumAtoms());
		assertEquals(4210, map.get("1HV4-BioAssembly2").getNumBonds());
	}
}
