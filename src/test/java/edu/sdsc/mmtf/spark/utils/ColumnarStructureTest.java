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

    @Test
    public void testGetChainEntityTypes() {
        StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
        String[] entityTypes = cs.getChainEntityTypes();
        assertEquals("PRO", entityTypes[0]);
        assertEquals("LGO", entityTypes[1]); // BTN
        assertEquals("WAT", entityTypes[2]); // HOH
    }

    @Test
    public void testGroupToAtomIndices() {
        StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
        int[] groupToAtomIndices = cs.getGroupToAtomIndices();
        assertEquals(0, groupToAtomIndices[0]); // ALA-13
        assertEquals(5, groupToAtomIndices[1]); // GLU-14
        assertEquals(14, groupToAtomIndices[2]); 
        assertEquals(1000, groupToAtomIndices[205]); // last HOH
        assertEquals(1001, groupToAtomIndices[206]); // end
    }

    @Test
    public void testChainToAtomIndices() {
        StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
        int[] chainToAtomIndices = cs.getChainToAtomIndices();
        assertEquals(0, chainToAtomIndices[0]); // chain A
        assertEquals(901, chainToAtomIndices[1]); // BTN
        assertEquals(917, chainToAtomIndices[2]); //HOH
        assertEquals(1001, chainToAtomIndices[3]); // end
    }

    @Test
    public void testChainToGroupIndices() {
        StructureDataInterface s = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(s, true);
        int[] chainToGroupIndices = cs.getChainToGroupIndices();
        assertEquals(0, chainToGroupIndices[0]); // chain A
        assertEquals(121, chainToGroupIndices[1]); // BTN
        assertEquals(122, chainToGroupIndices[2]); //HOH
        assertEquals(206, chainToGroupIndices[3]); // end
    }
}
