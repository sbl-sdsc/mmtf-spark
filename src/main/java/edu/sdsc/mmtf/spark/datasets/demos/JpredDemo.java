package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import edu.sdsc.mmtf.spark.datasets.JpredDataset;

/**
 * 
 * @author Yue YU
 * @since 0.1.0
 *
 */
public class JpredDemo {
	public static void main(String[] args) throws IOException {    
		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName(JpredDemo.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		Dataset<Row> ds = JpredDataset.getDataset(sc);

		ds.show(20, false);

		sc.close();
	}
}
