package edu.sdsc.mmtf.spark.io;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.GenericEncoder;
import org.rcsb.mmtf.encoder.WriterUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Encodes and writes MMTF encoded structure data to a Hadoop Sequence file. 
 * 
 * @author Peter Rose
 *
 */
public class MmtfWriter {

	/**
	 * Encodes and writes MMTF encoded and gzipped structure data to a Hadoop Sequence file.
	 * @param path Path to Hadoop file dictionary
	 * @param sc Spark context
	 * @param structure Structure data to be written
	 */
	public static void writeSequenceFile(String path, JavaSparkContext sc, JavaPairRDD<String, StructureDataInterface> structure) {		
		writeSequenceFile(path, sc,structure, true);
	}
	
	/**
	 * Encodes and writes MMTF encoded structure data to a Hadoop Sequence file.
	 * @param path Path to Hadoop file dictionary
	 * @param sc Spark context
	 * @param structure Structure data to be written
	 * @param compress if true, apply gzip compression
	 */
	public static void writeSequenceFile(String path, JavaSparkContext sc, JavaPairRDD<String, StructureDataInterface> structure, boolean compressed) {		
		structure
				.mapToPair(t -> new Tuple2<String,byte[]>(t._1, toByteArray(t._2, compressed)))
				.mapToPair(t -> new Tuple2<Text,BytesWritable>(new Text(t._1), new BytesWritable(t._2)))
				.saveAsHadoopFile(path, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
	}
	
	/**
	 * Encodes and writes MMTF encoded and gzipped structure data to individual .mmtf.gz files.
	 * @param path Path to dictionary where to save the individual files
	 * @param sc Spark context
	 * @param structure Structure data to be written
	 */
	public static void writeMmtfFiles(String path, JavaSparkContext sc, JavaPairRDD<String, StructureDataInterface> structure) {		
        if (! path.endsWith("/")) {
        	    path = path + "/";
        }
        final String fullPath = path;
          
        structure
				.mapToPair(t -> new Tuple2<String,byte[]>(t._1, toByteArray(t._2, true)))
		        .foreach(t -> FileUtils.writeByteArrayToFile(new File(fullPath+t._1+".mmtf.gz"), t._2));
	}
	
	/**
	 * Returns an MMTF-encoded byte array with optional gzip compression.
	 * @return MMTF encoded and optionally gzipped structure data
	 * @throws IOException
	 */
	private static byte[] toByteArray(StructureDataInterface structure, boolean compressed) throws IOException {
		GenericEncoder genericEncoder = new GenericEncoder(structure);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		MessagePackSerialization serializer = new MessagePackSerialization();
		serializer.serialize(genericEncoder.getMmtfEncodedStructure(), bos);
		if (compressed) {
			return  WriterUtils.gzipCompress(bos.toByteArray());
		} else {
			return bos.toByteArray();
		}
	}
}
