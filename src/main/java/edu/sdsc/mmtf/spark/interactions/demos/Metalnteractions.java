package edu.sdsc.mmtf.spark.interactions.demos;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.interactions.GroupInteractionExtractor;
import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.webfilters.Pisces;

/**
 * MetalInteractions calculates interactions of metal ions.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * @see edu.sdsc.mmtf.spark.utils.CoordinationGeometry
 *
 */
public class Metalnteractions {

    public static void main(String[] args) throws IOException {

        String path = MmtfReader.getMmtfFullPath();

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(Metalnteractions.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        // input parameters
        int sequenceIdentityCutoff = 30;
        double resolution = 2.5;
        int minInteractions = 4;
        int maxInteractions = 6;
        double distanceCutoff = 3.0;

        // chemical component codes of metals in different oxidation states
        String[] metals = {"V","CR","MN","MN3","FE","FE2","CO","3CO","NI","3NI",
                "CU","CU1","CU3","ZN","MO","4MO","6MO"};

        // read PDB and create a non-redundant PISCES subset
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc)
                .filter(new Pisces(sequenceIdentityCutoff, resolution));

        // Setup criteria for metal interactions
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(distanceCutoff);
        filter.setMinInteractions(minInteractions);
        filter.setMaxInteractions(maxInteractions);
        filter.setQueryGroups(true, metals);
        // exclude non-polar interactions
        filter.setTargetElements(false, "H", "C", "P");

        // tabulate interactions in a dataframe
        Dataset<Row> interactions = GroupInteractionExtractor.getInteractions(pdb, filter).cache();
        System.out.println("Metal interactions: " + interactions.count());

        // select interacting atoms and orientational order parameters (q4 - q6)
        // see {@link CoordinationGeometry}
        interactions = interactions.select("pdbId",
                "q4","q5","q6",
                "element0","groupNum0","chain0",
                "element1","groupNum1","chain1","distance1",
                "element2","groupNum2","chain2","distance2",
                "element3","groupNum3","chain3","distance3",
                "element4","groupNum4","chain4","distance4",
                "element5","groupNum5","chain5","distance5",
                "element6","groupNum6","chain6","distance6").cache();
        
        // show some example interactions
        interactions.dropDuplicates("pdbId").show(10);
        
        System.out.println("Unique interactions by metal:");
        interactions.groupBy("element0").count().sort("count").show();

        sc.close();
    }
}
