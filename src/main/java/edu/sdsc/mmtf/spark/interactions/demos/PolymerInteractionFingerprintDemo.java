package edu.sdsc.mmtf.spark.interactions.demos;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import edu.sdsc.mmtf.spark.interactions.InteractionFingerprinter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Demo how to calculate polymer-polymer interaction data.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class PolymerInteractionFingerprintDemo {

	public static void main(String[] args) throws Exception {		
		SparkSession spark = SparkSession.builder().master("local[*]")
				.appName(PolymerInteractionFingerprintDemo.class.getSimpleName()).getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> pdbIds = Arrays.asList("1OHR");
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);
        
        // find ASP-ARG salt bridges
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(3.5);
        filter.setMinInteractions(1);
        filter.setQueryGroups(true, "ASP");
        filter.setQueryAtomNames(true, "OD1", "OD2");
        filter.setTargetGroups(true, "ARG");
        filter.setTargetAtomNames(true, "NH1", "NH2");
		
		Dataset<Row> interactions = InteractionFingerprinter.getPolymerInteractions(pdb, filter).cache();
        interactions.show(false);
 
		sc.close();
	}
}