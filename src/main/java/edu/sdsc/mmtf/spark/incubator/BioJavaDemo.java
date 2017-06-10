package edu.sdsc.mmtf.spark.incubator;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.structure.Structure;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToBioJava;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.rcsbfilters.AdvancedQuery;
import edu.sdsc.mmtf.spark.rcsbfilters.BlastClusters;
import scala.Tuple2;

public class BioJavaDemo {

	public void test() throws IOException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(BioJavaDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
		 
	    long start = System.nanoTime();
	    List<String> pdbIds = Arrays.asList("1STP","4HHB","1JLP","5X6H","5L2G","2MK1");
	    // read PDB in MMTF format
//	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile("/Users/peter/MMTF_Files/full", sc);

	    // 1STP: 1 L-protein chain:
	    // 4HHB: 4 polymer chains
	    // 1JLP: 1 L-protein chains with non-polymer capping group (NH2)
	    // 5X6H: 1 L-protein and 1 DNA chain
	    // 5L2G: 2 DNA chain
	    // 2MK1: 1 D-saccharide
	    // --------------------
	    /// tot: 11 chains
	    String query = 
	    		"<orgPdbQuery>" +
	    		    "<queryType>org.pdb.query.simple.EnzymeClassificationQuery</queryType>" +
	    		    "<Enzyme_Classification>2.7.11.1</Enzyme_Classification>" +
	    		"</orgPdbQuery>";
	    
	    JavaPairRDD<String, Structure> structures = pdb
	    	.filter(new BlastClusters(40))
	    .flatMapToPair(new StructureToPolymerChains())
	    .filter(new BlastClusters(40))
	    .filter(new AdvancedQuery(query))
	    .mapValues(new StructureToBioJava()).cache();
	    
	    JavaPairRDD<String, Integer> polyCounts = structures.mapToPair(t -> new Tuple2<String,Integer>(t._1, t._2.getPolyChains().size()));
	   
//	    Atom[] calphas = StructureTools.getAtomCAArray(s);
//	    Point3d[] points = new Point3d[calphas.length];
//	    MomentsOfInertia moi = new MomentsOfInertia();
//	    for (Point3d p: points) {
//	     	moi.addPoint(p,  1.0);
//	    }
	    // moi.getRadiusOfGyration()
	    System.out.println(polyCounts.count());
	    
	    long end = System.nanoTime();
	    System.out.println((end-start)/1E9 + " sec.");
	    sc.close();
	}

}
