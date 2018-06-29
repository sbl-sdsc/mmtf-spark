package edu.sdsc.mmtf.spark.utils;

import static org.junit.Assert.*;

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

public class ColumnarStructureXTest {
    private JavaSparkContext sc;

    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ColumnarStructureTest.class.getSimpleName());
        sc = new JavaSparkContext(conf);

    }

    @After
    public void tearDown() throws Exception {
        sc.close();
    }

    @Test
    public void test1() {
        List<String> pdbIds = Arrays.asList("5NVB");
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
        StructureDataInterface s = pdb.values().first();
        ColumnarStructureX cs = new ColumnarStructureX(s, true);
        assertEquals(cs.getNormalizedbFactors()[0], Float.MAX_VALUE, 0.000001);
    }
    
    @Test
    public void test2() {
        List<String> pdbIds = Arrays.asList("4QXX");
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
        StructureDataInterface s = pdb.values().first();
        ColumnarStructureX cs = new ColumnarStructureX(s, true);
        assertTrue(cs.isGroupWithAlternateLocations()[6]);
    }
}
