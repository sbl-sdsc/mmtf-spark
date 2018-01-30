package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureSegmentExtractor;
import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

/**
 * @author Yue Yu
 * @since 0.1.0
 */
public class SecondaryStructureSegmentDemo {
	public static void main(String[] args) throws IOException {    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<String> pdbIds = Arrays.asList("1STP"); // single protein chain
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache();
	    
	    pdb = pdb
	    		.flatMapToPair(new StructureToPolymerChains())
	    		.filter(new ContainsLProteinChain());
	    
	    int segmentLength = 25;
	    Dataset<Row> ds = SecondaryStructureSegmentExtractor.getDataset(pdb, segmentLength);

	    // show the top 50 rows of this dataset
	    ds.show(50, false);

	    long end = System.nanoTime();
	    
	    System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end-start) + " sec.");
	    
	    sc.close();
	}
}