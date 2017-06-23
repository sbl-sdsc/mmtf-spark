package edu.sdsc.mmtf.spark.io.demos;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Check hadoop sequence file for lower case PDB IDs.
 * 
 * @author Peter Rose
 *
 */
public class PdbIdCaseCheck {

	public static void main(String[] args) {  

		String path = System.getProperty("MMTF_REDUCED");
		System.out.println(path);
		if (path == null) {
			System.err.println("Environment variable for Hadoop sequence file has not been set");
			System.exit(-1);
		}

		// instantiate Spark. Each Spark application needs these two lines of code.
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(PdbIdCaseCheck.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
		List<String> idsInValue = pdb.map(t -> t._2.getStructureId()).collect();
		List<String> idsInKey = pdb.keys().collect();

		System.out.println("Number of structures: " + idsInValue.size());

		for (String id: idsInValue) {
			if (id.equals(id.toLowerCase())) {
				System.err.println("lower case id in value: " + id);
			}
		}

		for (String id: idsInKey) {
			if (id.equals(id.toLowerCase())) {
				System.err.println("lower case id in key: " + id);
			}
		}

		// close Spark
		sc.close();
	}
}
