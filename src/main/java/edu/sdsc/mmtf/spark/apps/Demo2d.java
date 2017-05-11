package edu.sdsc.mmtf.spark.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import edu.sdsc.mmtf.spark.filters.ContainsDnaChain;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.ContainsPolymerType;
import edu.sdsc.mmtf.spark.filters.ContainsRnaChain;
import edu.sdsc.mmtf.spark.filters.NotFilter;
import edu.sdsc.mmtf.spark.io.MmtfSequenceFileReader;

/**
 * This example demonstrates how to filter the PDB by polymer chain type. It filters
 * 
 * Simple example of reading an MMTF Hadoop Sequence file, filtering the entries by resolution,
 * and counting the number of entries. This example shows how methods can be chained for a more
 * concise syntax.
 * 
 * @author Peter Rose
 *
 */
public class Demo2d {

	public static void main(String[] args) {

	    if (args.length != 1) {
	        System.err.println("Usage: " + Demo2d.class.getSimpleName() + " <hadoop sequence file>");
	        System.exit(1);
	    }
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Demo2d.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    
		 
	    MmtfSequenceFileReader
	    	.read(args[0], sc) // read MMTF hadoop sequence file
	    	// find chains that contain DNA, RNA, or both
	    	.filter(new ContainsPolymerType("DNA LINKING","RNA LINKING")) 
	    	.filter(new NotFilter(new ContainsDnaChain()))
	    	.filter(new NotFilter(new ContainsRnaChain()))
	    	.filter(new NotFilter(new ContainsLProteinChain()))
//	    	.sortByKey(Comparator.naturalOrder()) // or .reverseOrder() // this needs GenericDecoder to be serializable
            .keys()
	    	.foreach(key -> System.out.println(key));
	    
	    sc.close();
	}

}
