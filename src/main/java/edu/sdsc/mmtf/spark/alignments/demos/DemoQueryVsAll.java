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
 * protein structures against all protein chains.
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

        // default example
        List<String> queryIds = Arrays.asList("2W47");
        
        // use list of PDB IDs from the command line
        if (args.length > 0) {
            queryIds = Arrays.asList(args);
        }
        
        System.out.println("DemoQueryVsAll Query structures: " + queryIds);
        
        // download query structure
        JavaPairRDD<String, StructureDataInterface> query = MmtfReader.downloadReducedMmtfFiles(queryIds, sc)
                .flatMapToPair(new StructureToPolymerChains());

        // use a 25% random sample of the Pisces non-redundant set
        // at 20% sequence identity and a resolution better than 1.6 A.
        double fraction = 0.25;
        long seed = 11;
        JavaPairRDD<String, StructureDataInterface> target = MmtfReader.readSequenceFile(path, fraction, seed, sc)
                .flatMapToPair(new StructureToPolymerChains())
                .filter(new Pisces(20, 1.6))
                .sample(false, fraction, seed);

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

        // sort alignments by TM score
        alignments = alignments.sort(col("tm").desc());
        
        // show results
        int count = (int) alignments.count();
        alignments.show(count);
        System.out.println("Pairs: " + count);

        // save results to file
        alignments.write().mode("overwrite").option("compression", "gzip").parquet("alignments.parquet");
        
        long end = System.nanoTime();
        System.out.println("Time per alignment: " + TimeUnit.NANOSECONDS.toMillis((end - start) / count) + " msec.");
        System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + " sec.");

        sc.close();
    }
}
