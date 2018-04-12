package edu.sdsc.mmtf.spark.mappers;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
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

public class StructureToCathDomainsTest {
	private JavaSparkContext sc;
	
	@Before
	public void setUp() throws Exception {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(StructureToCathDomainsTest.class.getSimpleName());
	    sc = new JavaSparkContext(conf);
	}

	@After
	public void tearDown() throws Exception {
		sc.close();
	}

//	@Test
	public void test1() throws IOException {

		List<String> pdbIds = Arrays.asList("1HV4");
//		List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);

	    String baseUrl = "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/daily-release/newest/cath-b-newest-all.gz";

	    
//	    System.out.println(hmap.get("1HV4A"));
	      
	    JavaPairRDD<String, StructureDataInterface> cathDomains = pdb.flatMapToPair(new StructureToCathDomains(baseUrl));
	    
	    Map<String, ArrayList<String>> hmap = StructureToCathDomains.loadCathDomains(baseUrl);
        String[] bound = hmap.get("1HV4A").get(0).split(":")[0].split("-");

        int[] cath = cathDomains.first()._2.getGroupIds();       

        assertEquals(Integer.parseInt(bound[0]), cath[0]);
        
        assertEquals(Integer.parseInt(bound[1]), cath[cath.length-1]);
        
        assertEquals(Integer.parseInt(bound[1]) - Integer.parseInt(bound[0]) + 1, cathDomains.first()._2.getNumGroups());
                
        assertEquals(8, cathDomains.count());
	}
	
//	@Test
	public void test2() throws IOException {

		List<String> pdbIds = Arrays.asList("1STP");
//		List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);

	    String baseUrl = "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/daily-release/newest/cath-b-newest-all.gz";

	    
//	    System.out.println(hmap.get("1STPA"));
	      
	    JavaPairRDD<String, StructureDataInterface> cathDomains = pdb.flatMapToPair(new StructureToCathDomains(baseUrl));
	    
	    Map<String, ArrayList<String>> hmap = StructureToCathDomains.loadCathDomains(baseUrl);
        String[] bound = hmap.get("1STPA").get(0).split(":")[0].split("-");

        int[] cath = cathDomains.first()._2.getGroupIds();       

        assertEquals(Integer.parseInt(bound[0]), cath[0]);
        
        assertEquals(Integer.parseInt(bound[1]), cath[cath.length-1]);
        
        assertEquals(Integer.parseInt(bound[1]) - Integer.parseInt(bound[0]) + 1, cathDomains.first()._2.getNumGroups());
                
        assertEquals(1, cathDomains.count());
	}
}
