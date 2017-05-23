/**
 * 
 */
package edu.sdsc.mmtf.spark.incubator;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsDSaccharide;
import edu.sdsc.mmtf.spark.filters.ContainsPolymerType;
import edu.sdsc.mmtf.spark.filters.RcsbWebserviceFilter;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * @author peter
 *
 */
public class WebserviceChainDemo {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {

	    if (args.length != 1) {
	        System.err.println("Usage: " + WebserviceChainDemo.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WebserviceChainDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfSequenceFileReader.read(args[0], sc);
	    
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	  
//	    String whereClause = "WHERE pfamId='Pkinase_Tyr'";
//	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "pfamId"));
//	    String whereClause = "WHERE ecNo='2.7.11.1' AND source='Homo sapiens'";
//	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "ecNo","source"));
//	    String whereClause = "WHERE source='Homo sapiens'";
//	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "source"));
//	    String whereClause = "WHERE InChIKey='XPOQHMRABVBWPR-ZDUSSCGKSA-N'";
//	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "InChIKey"));
//	    String whereClause = "WHERE ligandMolecularWeight>=300 AND ligandMolecularWeight<=500";
//	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "ligandMolecularWeight"));
	    String whereClause = "WHERE pfamAccession LIKE 'PF07714%'"; 
	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "pfamAccession"));
//	    String whereClause = "WHERE pfamAccession IS NOT NULL"; 
//	    pdb = pdb.filter(new RcsbWebserviceFilter(whereClause, "pfamAccession"));
	

	    System.out.println(whereClause + ": " + pdb.count());

	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}

}
