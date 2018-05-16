package edu.sdsc.mmtf.spark.interactions.demos;

import java.util.Arrays;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import edu.sdsc.mmtf.spark.interactions.InteractionFingerprinter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * Demo how to calculate ligand-polymer interaction data and 
 * maps it to polymer chains.
 * 
 * @author Peter Rose
 * @since 0.2.0
 */
public class LigandInteractionFingerprintDemo {

    public static void main(String[] args) {
        // initialize Spark
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName(LigandInteractionFingerprintDemo.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> pdbIds = Arrays.asList("1OHR");
        
        // read a representative set of protein chains from the PDB
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadFullMmtfFiles(pdbIds, sc);

        // find interactions of small molecules (except water)
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(4.0);
        filter.setQueryGroups(false, "HOH"); // ignore water interactions

        Dataset<Row> interactions = InteractionFingerprinter.getLigandPolymerInteractions(pdb, filter);
        interactions.show(false);
    }
}
