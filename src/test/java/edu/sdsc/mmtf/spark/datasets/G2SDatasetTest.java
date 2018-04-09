package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class G2SDatasetTest {
    private SparkSession spark;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession.builder().master("local[*]").appName(G2SDatasetTest.class.getSimpleName()).getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        spark.close();
    }

    @Test
    public void test1() throws IOException {
        List<String> variantIds = Arrays.asList("chr7:g.140449098T>C");
        Dataset<Row> ds = G2SDataset.getPositionDataset(variantIds, "3TV4", "A");
        assertEquals(1, ds.filter("pdbPosition = 661").count());
    }

    @Test
    public void test2() throws IOException {
        List<String> variantIds = Arrays.asList("chr7:g.140449098T>C");
        Dataset<Row> ds = G2SDataset.getFullDataset(variantIds, "3TV4", "A");
        assertTrue(ds.filter("pdbPosition = 661").count() > 1);
    }

    @Test
    public void test3() throws IOException {
        List<String> variantIds = Arrays.asList("chr7:g.140449098T>C");
        Dataset<Row> ds = G2SDataset.getPositionDataset(variantIds, "1STP", "A");
        assertNull(ds);
    }

    @Test
    public void test4() throws IOException {
        List<String> variantIds = Arrays.asList("chr7:g.140449098T>C");
        Dataset<Row> ds = G2SDataset.getPositionDataset(variantIds);
        assertTrue(ds.count() > 5);
    }
}
