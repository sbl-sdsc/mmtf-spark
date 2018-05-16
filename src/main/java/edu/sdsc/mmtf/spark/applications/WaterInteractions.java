package edu.sdsc.mmtf.spark.applications;

import static org.apache.spark.sql.functions.col;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.filters.ContainsLProteinChain;
import edu.sdsc.mmtf.spark.filters.Resolution;
import edu.sdsc.mmtf.spark.interactions.ExcludedLigandSets;
import edu.sdsc.mmtf.spark.interactions.GroupInteractionExtractor;
import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import edu.sdsc.mmtf.spark.io.MmtfReader;

/**
 * WaterInteractions calculates interactions at the interface between ligands
 * and protein chains. Command line argument are used to specify criteria
 * for the interactions, such as distance cutoffs.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class WaterInteractions {

    public static void main(String[] args) throws IOException, ParseException {
        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmm").format(Calendar.getInstance().getTime());
        long start = System.nanoTime();
        
        // process command line options (defaults are provided)
        CommandLine cmd = getCommandLine(args);
        String outputPath = cmd.getOptionValue("output-path");
        System.out.println(outputPath);
        String resolution = cmd.getOptionValue("resolution", "2");
        String minInteractions = cmd.getOptionValue("min-interactions", "2");
        String maxInteractions = cmd.getOptionValue("max-interactions", "4");
        String distanceCutoff = cmd.getOptionValue("distance-cutoff", "3");
        String bFactorCutoff = cmd.getOptionValue("b-factor-cutoff", "1.645");
        boolean includeWaters = cmd.hasOption("include-waters");

        // get path to MMTF Hadoop Sequence file
        String path = MmtfReader.getMmtfFullPath();
        
        // initialize Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(WaterInteractions.class.getSimpleName());
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // read PDB structures and filter by resolution and only include proteins
        JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc)
                .filter(new Resolution(0.0, Float.parseFloat(resolution)))
                .filter(new ContainsLProteinChain(true));

        
        // setup interaction criteria
        InteractionFilter filter = new InteractionFilter();
        filter.setDistanceCutoff(Float.parseFloat(distanceCutoff));
        filter.setNormalizedbFactorCutoff(Float.parseFloat(bFactorCutoff));
        filter.setMinInteractions(Integer.parseInt(minInteractions));
        filter.setMaxInteractions(Integer.parseInt(maxInteractions));
        filter.setQueryGroups(true, "HOH");
        filter.setQueryElements(true, "O"); // only use water oxygen
        filter.setTargetElements(true, "O", "N", "S");
  
        // exclude "uninteresting" ligands
        Set<String> prohibitedGroups = new HashSet<>();
        prohibitedGroups.addAll(ExcludedLigandSets.ALL_GROUPS);
        if (!includeWaters) {
            prohibitedGroups.add("HOH");
        }
        filter.setProhibitedTargetGroups(prohibitedGroups);
     
        // calculate interactions
        Dataset<Row> data = GroupInteractionExtractor.getInteractions(pdb, filter);

        // keep only interactions with at least one organic ligand and one protein interaction
        data = filterBridgingWaterInteractions(data, maxInteractions).cache();
        
        // show some results
        data.show(50);
        System.out.println("Hits(all): " + data.count());
     
        // save interactions to a .parquet file
        String waterTag = includeWaters ? "_w" : "";
        String filename = outputPath + "/water_pl" + "_r" + resolution 
                + "_d" + distanceCutoff
                + "_b" + bFactorCutoff + "_i" + minInteractions + maxInteractions + waterTag + "_" + timeStamp + ".parquet";
        System.out.println("Saving results to: " + filename);
        data.coalesce(1).write().mode("overwrite").format("parquet").save(filename);

        
        // exit Spark
        sc.close();

        long end = System.nanoTime();
        System.out.println("Time: " + TimeUnit.NANOSECONDS.toSeconds(end - start) + " sec.");
    }

    /**
     * Remove rows where the water interaction does not include at least one organic ligand (LGO) 
     * and one protein residue (PRO).
     * 
     * TODO need to handle cases of maxInteractions > 4
     * @param data
     * @param maxInteractions
     * @return
     */
    private static Dataset<Row> filterBridgingWaterInteractions(Dataset<Row> data, String maxInteractions) {    
    	if (maxInteractions.compareTo("4") > 0) {
    		throw new IllegalArgumentException("maxInteractions > 4 are not supported, yet");
    	}
    	
        if (maxInteractions.equals("2")) {
            data = data.filter(col("type1").equalTo("LGO").or(col("type2").equalTo("LGO")));
            data = data.filter(col("type1").equalTo("PRO").or(col("type2").equalTo("PRO")));
        } else if (maxInteractions.equals("3")) {
            data = data.filter(col("type1").equalTo("LGO").or(col("type2").equalTo("LGO"))
                    .or(col("type3").equalTo("LGO")));
            data = data.filter(col("type1").equalTo("PRO").or(col("type2").equalTo("PRO"))
                    .or(col("type3").equalTo("PRO")));
        } else if (maxInteractions.equals("4")) {
            data = data.filter(col("type1").equalTo("LGO").or(col("type2").equalTo("LGO"))
                    .or(col("type3").equalTo("LGO")).or(col("type4").equalTo("LGO")));
            data = data.filter(col("type1").equalTo("PRO").or(col("type2").equalTo("PRO"))
                    .or(col("type3").equalTo("PRO")).or(col("type4").equalTo("PRO")));
        }
        return data;
    }
    
    private static CommandLine getCommandLine(String[] args) {
        Options options = new Options();

        options.addOption("h", "help", false, "help");
        options.addOption("o", "output-path", true, "path to output file");
        options.addOption("r", "resolution", true, "minimum resolution of structure");
        options.addOption("d", "distance-cutoff", true, "maximum distance for interactions");
        options.addOption("b", "b-factor-cutoff", true, "maximum normalized b-factor");
        options.addOption("min", "min-interactions", true, "minimum number of interactions");
        options.addOption("max", "max-interactions", true, "maximum number of interactions");
        options.addOption("w", "include-waters", false, "include water-water interactions");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("ERROR: invalid command line arguments: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(WaterInteractions.class.getSimpleName(), options);
            System.exit(-1);
        }

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(WaterInteractions.class.getSimpleName(), options);
            System.exit(1);
        }
        
        // TODO this check does not work??
//        if (!cmd.hasOption("output-path")) {
//            System.err.println("ERROR: no output path specified!");
//            HelpFormatter formatter = new HelpFormatter();
//            formatter.printHelp(WaterInteractions.class.getSimpleName(), options);
//            System.exit(1);
//        }

        return cmd;
    }
}
