package edu.sdsc.mmtf.spark.interactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
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

import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import edu.sdsc.mmtf.spark.interactions.InteractionFingerprinter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

public class PolymerInteractionFingerprintTest {
    private JavaSparkContext sc;
    
    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PolymerInteractionFingerprintTest.class.getSimpleName());
        sc = new JavaSparkContext(conf);
    }

    @After
    public void tearDown() throws Exception {
        sc.close();
    }
    @Test
    public void test1() {
        List<String> pdbIds = Arrays.asList("1OHR");
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
        
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(4.0);
        filter.setMinInteractions(10);
        
        Dataset<Row> interactions = InteractionFingerprinter.getPolymerInteractions(pdb, filter).cache();
        assertEquals(2, interactions.count());
        interactions.show(false);
    }
    
    @Test
    public void test2() throws IOException {
        List<String> pdbIds = Arrays.asList("1OHR");
        
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
        
        // find ASP-ARG salt bridges
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(3.5);
        filter.setMinInteractions(1);
        filter.setQueryGroups(true, "ASP");
        filter.setQueryAtomNames(true, "OD1", "OD2");
        filter.setTargetGroups(true, "ARG");
        filter.setTargetAtomNames(true, "NH1", "NH2");

        Dataset<Row> interactions = InteractionFingerprinter.getPolymerInteractions(pdb, filter);
        assertEquals(2, interactions.count());
    }
}
