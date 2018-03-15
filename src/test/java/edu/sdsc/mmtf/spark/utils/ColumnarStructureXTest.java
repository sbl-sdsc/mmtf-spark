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
    private JavaPairRDD<String, StructureDataInterface> pdb;

    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ColumnarStructureTest.class.getSimpleName());
        sc = new JavaSparkContext(conf);

        List<String> pdbIds = Arrays.asList("5NVB");
        pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
    }

    @After
    public void tearDown() throws Exception {
        sc.close();
    }

    @Test
    public void test1() {
        StructureDataInterface s = pdb.values().first();
        ColumnarStructureX cs = new ColumnarStructureX(s, true);
        assertEquals(cs.getNormalizedbFactors()[0], Float.MAX_VALUE, 0.000001);
    }
}
