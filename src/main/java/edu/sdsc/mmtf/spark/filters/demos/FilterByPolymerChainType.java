package edu.sdsc.mmtf.spark.filters.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsDSaccharideChain;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.ContainsPolymerChainType;
import edu.sdsc.mmtf.spark.filters.NotFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * This example demonstrates how to filter the PDB by polymer chain type. It filters
 * 
 * Simple example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution,
 * and counting the number of entries. This example shows how methods can be chained for a more
 * concise syntax.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class FilterByPolymerChainType {

	public static void main(String[] args) throws FileNotFoundException {

		String path = MmtfReader.getMmtfReducedPath();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(FilterByPolymerChainType.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
	    long count = MmtfReader
	    	.readSequenceFile(path, sc) // read MMTF hadoop sequence file
	    	 // find chains that contain DNA, RNA, or both
	    	.filter(new ContainsPolymerChainType("DNA LINKING","RNA LINKING")) 
	    	.filter(new NotFilter(new ContainsLProteinChain()))
	    	.filter(new NotFilter(new ContainsDSaccharideChain()))
	    .count();
	    
	    System.out.println("# pure DNA and RNA entries: " + count);
	    sc.close();
	}

}
