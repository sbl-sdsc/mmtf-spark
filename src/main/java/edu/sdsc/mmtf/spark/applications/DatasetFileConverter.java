package edu.sdsc.mmtf.spark.applications;

import java.io.File;
import java.io.IOException;

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

import com.google.common.io.Files;

/**
 * Tool to convert Spark Dataframes (Datasets) among various file formats and
 * compression codecs. The number of partitions can be specified. 
 * If one (1) partition is specified, the output is a single file and 
 * if > 1 the output is a directory with multiple files
 * named part-00000..., part-00001..., etc.. If no partition is specified, the
 * default number of partitions is used.
 * 
 * <p>Supported file format and (compression codecs)
 * <pre>
 * parquet (gzip, snappy)
 * orc     (lzo, zlib)
 * </pre>
 * 
 * <p>Example 1:
 * <pre>
 * {@code
 * DatasetFileConverter -i demo.parquet.gzip -f orc -c lzo
 * }
 * </pre>
 * This example creates a single output file:
 * <pre>
 * /demo.orc.lzo
 * </pre>
 * <p>Example 2: (with 12 partitions specified)
 * <pre>
 * {@code
 * DatasetFileConverter -i demo.parquet.gzip -f orc -c lzo -p 12
 * }
 * </pre>
 * This example creates an output directory with 12 parts:
 * <pre>
 * /demo.12.orc.lzo
 *   part-00000-...orc.lzo
 *   ...
 *   part-00011-...orc.lzo
 * </pre>
 *
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class DatasetFileConverter {

    public static void main(String[] args) throws IOException {

        // process command line options (defaults are provided)
        CommandLine cmd = getCommandLine(args);
        String inputFile = cmd.getOptionValue("input-file");
        int partitions = Integer.parseInt(cmd.getOptionValue("partitions", "0"));
        String fileFormat = cmd.getOptionValue("file-format", "");
        String compressionCodec = cmd.getOptionValue("compression-codec", "");

        SparkSession spark = SparkSession.builder().master("local[*]")
                .appName(DatasetFileConverter.class.getSimpleName())
                .getOrCreate();

        spark.conf().set("spark.sql.orc.impl", "native");

        // encode options in file name
        String outputFile = getFileExtension(inputFile);
        if (partitions > 1) {
            outputFile += "." + partitions;
        }
        outputFile += "." + fileFormat;
        if (!compressionCodec.isEmpty()) {
            outputFile += "." + compressionCodec;
        }

        System.out.println("Input file : " + inputFile);
        System.out.println("Output file: " + outputFile);

        long t1 = System.nanoTime();

        Dataset<Row> dataset = null;
        
        // read dataset
        if (inputFile.contains("orc")) {
            dataset = spark.read().format("orc").load(inputFile);
        } else if (inputFile.contains("csv")) {
            dataset = spark.read().format("csv").load(inputFile);
        } else {
            dataset = spark.read().format("parquet").load(inputFile);
        }
        
        long records = dataset.count();

        // write reformatted dataset
        saveDataset(dataset, partitions, fileFormat, compressionCodec, outputFile);

        long t2 = System.nanoTime();

        System.out.println(records + " records reformatted in " + (t2-t1)/1E9 + " sec.");

        spark.stop();
    }

    /**
     * Saves a Spark dataset in supported file formats and compression codecs.
     * The number of partitions (number of parts a file is split into) can be specified.
     * 
     * <p>Partitions:
     * <pre>
     * 0    use the default number of partitions
     * 1    create 1 partition and save as a single output file
     * >1   create the specified number of partitions and save in output directory
     * </pre>
     * 
     * <p>Supported file format and (compression codecs)
     * <pre>
     * parquet (gzip, snappy, uncompressed)
     * orc     (lzo, zlib, snappy, uncompressed)
     * </pre>
     * 
     * @param dataset dataset to be saved
     * @param partitions number of partitions
     * @param fileFormat file format
     * @param compressionCodec codec used to compress data or empty string to specify no compression
     * @param outputFile name of output file or output directory
     * @throws IOException
     */
    public static void saveDataset(Dataset<Row> dataset, int partitions, String fileFormat, String compressionCodec, String outputFile) throws IOException {

        if (partitions == 0) {
            // no partition specified, do not repartition data
            if (compressionCodec.isEmpty()) {
                dataset.write().mode("overwrite").format(fileFormat).save(outputFile);
            } else {
                dataset.write().mode("overwrite").option("compression", compressionCodec).format(fileFormat)
                .save(outputFile);
            }
        } else if (partitions == 1) {
            // create a single partition and save as a single file
            if (compressionCodec.isEmpty()) {
                dataset.coalesce(1).write().mode("overwrite").format(fileFormat).save(outputFile + "_tmp");
            } else {
                dataset.coalesce(1).write().mode("overwrite").option("compression", compressionCodec).format(fileFormat)
                .save(outputFile + "_tmp");
            }
            moveToSingleFile(outputFile + "_tmp", outputFile);
            FileUtils.deleteDirectory(new File(outputFile + "_tmp"));
        } else if (partitions > 0) {
            // write specified partitions to a directory
            if (compressionCodec.isEmpty()) {
                dataset.coalesce(partitions).write().mode("overwrite").format(fileFormat).save(outputFile);
            } else {
                dataset.coalesce(partitions).write().mode("overwrite").option("compression", compressionCodec)
                .format(fileFormat).save(outputFile);
            }
        }
    }

    private static void moveToSingleFile(String dirname, String filename) throws IOException {
        File part = getPartsFile(dirname);
        if (part != null) {
            File singleFile = new File(filename);
            Files.move(part, singleFile);
            FileUtils.deleteDirectory(new File(dirname));
        }
    }

    private static File getPartsFile(String dirname) {
        File folder = new File(dirname);

        for (File file: folder.listFiles()) {
            if (file.isFile() && file.getName().startsWith("part")) {
                return file;
            }
        }
        return null;
    }

    private static CommandLine getCommandLine(String[] args) {
        Options options = new Options();

        options.addOption("h", "help", false, "help");
        options.addOption("i", "input-file", true, "path to input file");
        options.addOption("p", "partitions", true, "number of partions, creates single file if n=1");       
        options.addOption("f", "file-format", true, "parquet, orc, csv");
        options.addOption("c", "compression-codec", true, "gzip or snappy for parquet, zlib or lzo for orc");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println("ERROR: invalid command line arguments: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(DatasetFileConverter.class.getSimpleName(), options);
            System.exit(-1);
        }

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(DatasetFileConverter.class.getSimpleName(), options);
            System.exit(1);
        }

        if (!cmd.hasOption("input-file")) {
            System.err.println("ERROR: no input file specified!");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(DatasetFileConverter.class.getSimpleName(), options);
            System.exit(-1);
        }
        
        if (!cmd.hasOption("file-format")) {
            System.err.println("ERROR: no file format specified!");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(DatasetFileConverter.class.getSimpleName(), options);
            System.exit(-1);
        }

        return cmd;
    }

    private static String getFileExtension(String fileName) {
        int index = fileName.indexOf(".");
        if (index < 0) {
            return fileName;
        } else {
            return fileName.substring(0, index);
        }
    }
}
