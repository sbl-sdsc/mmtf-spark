package edu.sdsc.mmtf.spark.applications;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.PdbToUniProt;

/**
 * Builds or updates a dataset of PDB to UniProt residue number mappings from 
 * the SIFTS project. Building a new dataset is very slow and may take more 
 * than one day. Preferably, use the update option (-u) to update the cached dataset.
 * 
 * For more information about SIFTS see:
 * <p>
 * The "Structure Integration with Function, Taxonomy and Sequence"
 * (<a href="https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html">SIFTS</a>) is
 * the authoritative source of up-to-date residue-level mapping to UniProt.
 *
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class CreatePdbToUniProtMappingFile {
   
    public static void main(String[] args) throws IOException, InterruptedException {
        
        // process command line options (defaults are provided)
        CommandLine cmd = getCommandLine(args);
        String outputFile = cmd.getOptionValue("output-file");
        boolean build = cmd.hasOption("build");
        boolean update = cmd.hasOption("update");
        
        // these default options for fileFormat and compressionCodec 
        // provide the best compression
        String fileFormat = cmd.getOptionValue("file-format", "orc");
        String compressionCodec = cmd.getOptionValue("compression-codec", "lzo");
   
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName(CreatePdbToUniProtMappingFile.class.getSimpleName())
                .getOrCreate();
        
        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        long t1 = System.nanoTime();
        
        String dirName = outputFile + "_" + timeStamp + "_tmp";
        String fileName = outputFile + "_" + timeStamp + "." + fileFormat + "." + compressionCodec;
        
        if (build) {
            // create a new mapping file from scratch
            PdbToUniProt.buildDataset(dirName, "orc", "lzo");
        } else if (update) {
            // create an updated mapping file from the cached version
            PdbToUniProt.updateDataset(dirName, "orc", "lzo");
        }

        long t2 = System.nanoTime();
        System.out.println("Time to build/update dataset: " + (t2-t1)/1E9 + " sec.");
               
        // By default, spark creates a directory of files. 
        // For convenience, coalesce the data into a single file.
        Dataset<Row> ds = spark.read().orc(dirName);
        long count = ds.count();
        
        int partitions = 1;
        DatasetFileConverter.saveDataset(ds, partitions, fileFormat, compressionCodec, fileName);
        FileUtils.deleteDirectory(new File(dirName));
        
        System.out.println(count + " records saved to: " + fileName);
        
        long t3 = System.nanoTime();
        System.out.println("Time to reformat data: " + (t3-t2)/1E9 + " sec.");

        spark.stop();
    }
    
    private static CommandLine getCommandLine(String[] args) {
        Options options = new Options();

        options.addOption("h", "help", false, "help");
        options.addOption("o", "output-file", true, "path to output file");
        options.addOption("b", "build", false, "build a new dataset (slow!)");
        options.addOption("u", "update", false, "update cached dataset");
        options.addOption("f", "file-format", true, "parquet, orc");
        options.addOption("c", "compression-codec", true, "gzip or snappy for parquet, zlib or lzo for orc");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("ERROR: invalid command line arguments: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CreatePdbToUniProtMappingFile.class.getSimpleName(), options);
            System.exit(-1);
        }

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CreatePdbToUniProtMappingFile.class.getSimpleName(), options);
            System.exit(1);
        }
        
        if (!cmd.hasOption('b') && !cmd.hasOption('u')) {
            System.out.println("ERROR: use either -u or -b option");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CreatePdbToUniProtMappingFile.class.getSimpleName(), options);
            System.exit(-1);
        }
        
        if (cmd.hasOption('b') && cmd.hasOption('u')) {
            System.out.println("ERROR: use either -u or -b option");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CreatePdbToUniProtMappingFile.class.getSimpleName(), options);
            System.exit(-1);
        }
        
        if (!cmd.hasOption("output-file")) {
            System.err.println("ERROR: no output file specified!");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CreatePdbToUniProtMappingFile.class.getSimpleName(), options);
            System.exit(1);
        }

        return cmd;
    }
}
