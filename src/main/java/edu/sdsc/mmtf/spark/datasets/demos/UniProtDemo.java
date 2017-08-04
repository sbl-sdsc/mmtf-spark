package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import edu.sdsc.mmtf.spark.datasets.Uniprot;
import edu.sdsc.mmtf.spark.datasets.Uniprot.UniDataset;

public class UniProtDemo {
	public static void main(String[] args) throws IOException {  
		if (args.length != 2) {
			System.err.println("Usage: " + Uniprot.class.getSimpleName() + " <outputFilePath> + <fileFormat>");
			System.exit(1);
		}
	    long start = System.nanoTime();
	    
	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(CustomReportDemo.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    Dataset<Row> ds = Uniprot.getDataset(sc, UniDataset.UNIREF100);
	    // show the schema of this dataset
	    ds.printSchema();
	    ds.show(20, false);
	    ds.write().mode("overwrite").format(args[1]).save(args[0]);
	    
	    System.out.println("Count: " + ds.count());
	    long end = System.nanoTime();
	    
	    System.out.println("Time:     " + (end-start)/1E9 + "sec.");
	    
	    sc.close();
	}
}
