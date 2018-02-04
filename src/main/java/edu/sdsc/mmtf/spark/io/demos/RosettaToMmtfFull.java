package edu.sdsc.mmtf.spark.io.demos;

import java.io.FileNotFoundException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.MmtfWriter;

/**
 * Converts a directory containing PDB files into an MMTF-Hadoop Sequence file
 * with "full" (all atom, full precision) representation. The input directory 
 * is traversed recursively to find PDB files.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class RosettaToMmtfFull {

    /**
     * Converts a directory containing Rosetta-style PDB files into an MMTF-Hadoop Sequence file.
     * The input directory is traversed recursively to find PDB files.
     * 
     * <p> Example files from Gremlin website:
     * https://gremlin2.bakerlab.org/meta/aah4043_final.zip
     * 
     * @param args args[0] <path-to-pdb_files>, args[1] <path-to-mmtf-hadoop-file>
     * 
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException {  

        if (args.length != 2) {
            System.out.println("Usage: RosettaToMmtfFull <path-to-pdb_files> <path-to-mmtf-hadoop-file>");
        }

        // path to input directory
        String pdbPath = args[0];

        // path to output directory
        String mmtfPath = args[1];

        // instantiate Spark
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("RosettaToMmtfFull");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // read PDB files recursively starting the specified directory
        JavaPairRDD<String, StructureDataInterface> structures = MmtfReader.readRosettaPdbFiles(pdbPath, sc);

        // save as an MMTF-Hadoop Sequence File
        MmtfWriter.writeSequenceFile(mmtfPath, sc, structures);

        // close Spark
        sc.close(); 
    }
}
