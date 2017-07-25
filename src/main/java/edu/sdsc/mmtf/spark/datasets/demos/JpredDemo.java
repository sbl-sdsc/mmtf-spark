package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import edu.sdsc.mmtf.spark.datasets.JpredDataset;

public class JpredDemo {
	public static void main(String[] args) throws IOException {    
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(JpredDemo.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		Dataset<Row> res = JpredDataset.getDataset(sc);

		res.show(20, false);
		res = res.coalesce(1);
		res.write().mode("overwrite").format("json").save("D:/jPredData3");
		

		sc.close();
	}
}
