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

public class MyVariantDatasetTest {
    private SparkSession spark;

    @Before
    public void setUp() throws Exception {
        spark = SparkSession.builder().master("local[*]").appName(MyVariantDatasetTest.class.getSimpleName()).getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        spark.close();
    }

    @Test
    public void test1() throws IOException {
        List<String> uniprotIds = Arrays.asList("P00533"); // EGFR
        Dataset<Row> ds = MyVariantDataset.getVariations(uniprotIds);
        assertTrue(ds.count() > 7000);
    }
    
    @Test
    public void test2() throws IOException {
        List<String> uniprotIds = Arrays.asList("P15056"); // BRAF
        String query = "clinvar.rcv.clinical_significance:pathogenic OR clinvar.rcv.clinical_significance:likely pathogenic";
        Dataset<Row> ds = MyVariantDataset.getVariations(uniprotIds, query);
        assertEquals(1, ds.filter(
                "variationId = 'chr7:g.140501287T>C'"
                + " AND uniprotId = 'P15056'").count());
    }
}
