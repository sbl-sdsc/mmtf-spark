package edu.sdsc.mmtf.spark.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.anarres.lzo.hadoop.codec.LzoCodec;
import org.anarres.lzo.hadoop.codec.LzoCompressor;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.GenericEncoder;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Encodes and writes an MMTF Hadoop Sequence file. 
 * @author Peter Rose
 *
 */
public class MmtfSequenceFileWriter {

	public static void write(String path, JavaSparkContext sc, JavaPairRDD<String, StructureDataInterface> structure) {		
		JavaPairRDD<Text, BytesWritable> rdd = structure
				.mapToPair(t -> new Tuple2<String,byte[]>(t._1, toByteArray(t._2)))
	//			.repartition(4)
				.mapToPair(t -> new Tuple2<Text,BytesWritable>(new Text(t._1), new BytesWritable(t._2)));
		rdd.saveAsHadoopFile(path, Text.class, BytesWritable.class, SequenceFileOutputFormat.class);
	}
	
	/**
	 * Returns an MMTF encoded byte array (MessagePacked serialized).
	 * @return MMTF encoded data
	 * @throws IOException
	 */
	private static byte[] toByteArray(StructureDataInterface structure) throws IOException {
		MessagePackSerialization mmtfBeanSeDerializerInterface = new MessagePackSerialization();
		GenericEncoder genericEncoder = new GenericEncoder(structure);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		mmtfBeanSeDerializerInterface.serialize(genericEncoder.getMmtfEncodedStructure(), bos);
		return bos.toByteArray();
	}
}
