package edu.sdsc.mmtf.spark.alignments.demos;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.alignments.StructureAligner;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToPolymerChains;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * This class demonstrates how to run a structural alignment of a set of
 * protein chains against all protein chains.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class DemoQueryVsAll {

    public static void main(String[] args) throws IOException {

        String path = MmtfReader.getMmtfReducedPath();

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(DemoQueryVsAll.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.nanoTime();

        // download query structure
        List<String> queryId = Arrays.asList("2W47");
        JavaPairRDD<String, StructureDataInterface> query = MmtfReader.downloadFullMmtfFiles(queryId, sc)
                .flatMapToPair(new StructureToPolymerChains());

        // use a 1 % sample of the PDB and then filter by the Pisces
        // non-redundant set
        // at 20% sequence identity and a resolution better than 1.6 A.
        double fraction = 1.0;
        long seed = 123;
        JavaPairRDD<String, StructureDataInterface> target = MmtfReader.readSequenceFile(path, fraction, seed, sc)
                .flatMapToPair(new StructureToPolymerChains())
                .filter(new Pisces(20, 1.6))
                .sample(false, 0.08, seed);

        // specialized algorithms
        // String alignmentAlgorithm = CeMain.algorithmName;
        // String alignmentAlgorithm = CeCPMain.algorithmName;
        // String alignmentAlgorithm = FatCatFlexible.algorithmName;

        // two standard algorithms
        // String alignmentAlgorithm = CeMain.algorithmName;
        String alignmentAlgorithm = FatCatRigid.algorithmName;

        // String alignmentAlgorithm = ExhaustiveAligner.alignmentAlgorithm;

        // calculate alignments
        Dataset<Row> alignments = StructureAligner.getQueryVsAllAlignments(query, target, alignmentAlgorithm).cache();

        // show results
        int count = (int) alignments.count();
        alignments.sort(col("tm").desc()).show(count);
        System.out.println("Pairs: " + count);

        long end = System.nanoTime();
        System.out.println("Time per alignment: " + TimeUnit.NANOSECONDS.toMillis((end - start) / count) + " msec.");
        System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + " sec.");

        sc.close();
    }
}
