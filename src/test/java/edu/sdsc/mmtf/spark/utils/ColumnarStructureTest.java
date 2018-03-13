package edu.sdsc.mmtf.spark.utils;

import static org.junit.Assert.assertEquals;

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
import edu.sdsc.mmtf.spark.utils.ColumnarStructure;

public class ColumnarStructureTest {
	private JavaSparkContext sc;
	private JavaPairRDD<String, StructureDataInterface> pdb;

	@Before
	public void setUp() throws Exception {
      	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ColumnarStructureTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);

	    List<String> pdbIds = Arrays.asList("1STP");
		pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

	@Test
	public void testGetxCoords() {
		StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
		assertEquals(26.260, cs.getxCoords()[20], 0.001);
	}
	
	@Test
	public void testGetElements() {
		StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
		assertEquals("C", cs.getElements()[20]);
	}
	
	@Test
	public void testGetAtomNames() {
		StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
		assertEquals("CG2", cs.getAtomNames()[900]);
	}
	
	@Test
	public void testGetGroupNames() {
		StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
		assertEquals("VAL", cs.getGroupNames()[900]);
	}
	
	@Test
	public void testIsPolymer() {
		StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
		assertEquals(true, cs.isPolymer()[100]); // chain A
		assertEquals(false, cs.isPolymer()[901]); // BTN
		assertEquals(false, cs.isPolymer()[917]); // HOH
	}

	@Test
	public void testGetGroupNumbers() {
			StructureDataInterface s = pdb.values().first();
	        ColumnarStructure cs = new ColumnarStructure(s, true);
			assertEquals("130", cs.getGroupNumbers()[877]);
	}
	
	@Test
	public void testGetChainIds() {
			StructureDataInterface s = pdb.values().first();
	        ColumnarStructure cs = new ColumnarStructure(s, true);
			assertEquals("A", cs.getChainIds()[100]);
			assertEquals("B", cs.getChainIds()[901]); // BTN
			assertEquals("C", cs.getChainIds()[917]); // HOH
	}
	
	@Test
	public void testGetChemCompTypes() {
			StructureDataInterface s = pdb.values().first();
	        ColumnarStructure cs = new ColumnarStructure(s, true);
			assertEquals("PEPTIDE LINKING", cs.getChemCompTypes()[100]);
			assertEquals("NON-POLYMER", cs.getChemCompTypes()[901]); // BTN
			assertEquals("NON-POLYMER", cs.getChemCompTypes()[917]); // HOH
	}
	
	@Test
	public void testGetEntityTypes() {
			StructureDataInterface s = pdb.values().first();
	        ColumnarStructure cs = new ColumnarStructure(s, true);
			assertEquals("PRO", cs.getEntityTypes()[100]);
			assertEquals("LGO", cs.getEntityTypes()[901]); // BTN
			assertEquals("WAT", cs.getEntityTypes()[917]); // HOH
	}
}
