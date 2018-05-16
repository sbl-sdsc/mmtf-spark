package edu.sdsc.mmtf.spark.utils;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.utils.ColumnarStructure;
import edu.sdsc.mmtf.spark.utils.CoordinationGeometry;

public class CoordinationGeometryTest {
    private JavaSparkContext sc;
    private JavaPairRDD<String, StructureDataInterface> pdb;

    @Before
    public void setUp() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ColumnarStructureTest.class.getSimpleName());
        sc = new JavaSparkContext(conf);

        List<String> pdbIds = Arrays.asList("5Y20");
        pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
    }

    @After
    public void tearDown() throws Exception {
        sc.close();
    }

    @Test
    public void test() {
        StructureDataInterface structure = pdb.values().first();
        ColumnarStructure cs = new ColumnarStructure(structure, true);
 
        Point3d center = getCoords(cs, 459); // ZN A.101.ZN

        Point3d[] neighbors = new Point3d[6];
        neighbors[0] = getCoords(cs, 28); // CYS A.7.SG
        neighbors[1] = getCoords(cs, 44); // CYS A.10.SG
        neighbors[2] = getCoords(cs, 223); // HIS A.31.ND1
        neighbors[3] = getCoords(cs, 245); // CYS A.34.SG
        neighbors[4] = getCoords(cs, 45); // CYS A.10.N
        neighbors[5] = getCoords(cs, 220); // HIS A.31.O
        
        CoordinationGeometry geom = new CoordinationGeometry(center, neighbors);
        
        double q3Expected = 0.9730115379131878;
        assertEquals(q3Expected, geom.q3(), 0.0001);
        
        double q4Expected = 0.9691494056145086;
        assertEquals(q4Expected, geom.q4(), 0.0001); 
        
        double q5Expected = 0.5126001729084566;
        assertEquals(q5Expected, geom.q5(), 0.0001); 
        
        double q6Expected = 0.2723305441457363;
        assertEquals(q6Expected, geom.q6(), 0.0001); 
    }
    
    private static Point3d getCoords(ColumnarStructure cs, int index) {
        return new Point3d(
                cs.getxCoords()[index],
                cs.getyCoords()[index],
                cs.getzCoords()[index]);
    }
}
