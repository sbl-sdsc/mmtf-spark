package edu.sdsc.mmtf.spark.io.demos;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Example reading a list of PDB IDs from a local 
 * reduced MMTF Hadoop sequence file into a JavaPairRDD.
 * 
 * @author Peter Rose
 *
 */
public class TestReduced {

	public static void main(String[] args) {  

		String path = System.getProperty("MMTF_REDUCED");
		System.out.println(path);
		if (path == null) {
			System.err.println("Environment variable for Hadoop sequence file has not been set");
			System.exit(-1);
		}

		// instantiate Spark. Each Spark application needs these two lines of code.
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(TestReduced.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
		List<String> pdbIds = pdb.keys().collect();

		System.out.println("Number of structures: " + pdbIds.size());

		for (String id: pdbIds) {
			if (id.equals(id.toLowerCase())) {
				System.err.println("lower case id: " + id);
			}

			// close Spark
			sc.close();
		}
	}
}
