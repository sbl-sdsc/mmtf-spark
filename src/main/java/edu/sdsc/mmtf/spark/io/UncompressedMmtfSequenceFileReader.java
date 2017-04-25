package edu.sdsc.mmtf.spark.io;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Reads and decodes an MMTF Hadoop Sequence file.
 * @author Peter Rose
 *
 */
public class UncompressedMmtfSequenceFileReader {

	public static JavaPairRDD<String, StructureDataInterface>  read(String fileName, JavaSparkContext sc) {
		return sc
				.sequenceFile(fileName, Text.class, BytesWritable.class)
				.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>,String, StructureDataInterface>() {
					private static final long serialVersionUID = -889818426359549298L;

					public Tuple2<String, StructureDataInterface> call(Tuple2<Text, BytesWritable> t) throws Exception {
						MmtfStructure deserialize = new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2.copyBytes())); // deserialize message pack
						return new Tuple2<String, StructureDataInterface>(t._1.toString(), new GenericDecoder(deserialize)); // decode message pack
					}
				});
	}
}
