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
 * Encodes and writes an MMTF Hadoop Sequence file. 
 * @author Peter Rose
 *
 */
public class MmtfWriter {

	public static void writeSequenceFile(String path, JavaSparkContext sc, JavaPairRDD<String, StructureDataInterface> structure) {		
		structure
				.mapToPair(t -> new Tuple2<String,byte[]>(t._1, toGzippedByteArray(t._2)))
				.mapToPair(t -> new Tuple2<Text,BytesWritable>(new Text(t._1), new BytesWritable(t._2)))
				.saveAsHadoopFile(path, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
	}
	
	/**
	 * @param path
	 * @param sc
	 * @param structure
	 */
	public static void writeMmtfFiles(String path, JavaSparkContext sc, JavaPairRDD<String, StructureDataInterface> structure) {		
        if (! path.endsWith("/")) {
        	path = path + "/";
        }
        final String fullPath = path;
        
		structure
				.mapToPair(t -> new Tuple2<String,byte[]>(t._1, toGzippedByteArray(t._2)))
		        .foreach(t -> FileUtils.writeByteArrayToFile(new File(fullPath+t._1+".mmtf.gz"), t._2));
	}
	
	/**
	 * Returns a gzipped MMTF-encoded byte array.
	 * @return MMTF encoded data
	 * @throws IOException
	 */
	private static byte[] toGzippedByteArray(StructureDataInterface structure) throws IOException {
		GenericEncoder genericEncoder = new GenericEncoder(structure);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		MessagePackSerialization serializer = new MessagePackSerialization();
		serializer.serialize(genericEncoder.getMmtfEncodedStructure(), bos);
		return  WriterUtils.gzipCompress(bos.toByteArray());
	}
}
