package edu.sdsc.mmtf.spark.datasets;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

public class QuaternaryStructureDatasetTest {
    private JavaSparkContext sc;
    private JavaPairRDD<String, StructureDataInterface> pdb;
    
    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PolymerSequenceExtractorTest.class.getSimpleName());
        sc = new JavaSparkContext(conf);

        List<String> pdbIds = Arrays.asList("1STP","4HHB","1HV4","5NAO","5OR7","1A34","5W34");
        pdb = MmtfReader.downloadReducedMmtfFiles(pdbIds, sc);
    }

    @After
    public void tearDown() throws Exception {
        sc.close();
    }

    @Test
    public void test1() {
        Dataset<Row> dataset = QuaternaryStructureDataset.getDataset(pdb);
        assertEquals(13,  dataset.count());
        dataset.show();
    }
}
