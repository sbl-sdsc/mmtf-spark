package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.SecondaryStructureExtractor;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;

public class DSSPDemo {
	public static void main(String[] args) throws IOException {    
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    List<String> pdbIds = Arrays.asList("1STP"); // single protein chain
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache(); 
	    pdb = pdb.flatMapToPair(new StructureToPolymerChains());
	    Dataset<Row> ds = SecondaryStructureExtractor.getDataset(pdb);
	    
	    // show the schema of this dataset
	    ds.printSchema();
	    ds.show(2, false);

	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
