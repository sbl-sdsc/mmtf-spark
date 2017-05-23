package edu.sdsc.mmtf.spark.io;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Reads and decodes an MMTF Hadoop Sequence file. It returns a JavaPairRDD with the structure id
 * (e.g. PDB ID) as the key and the MMTF StructureDataInterface as the value.
 * @author Peter Rose
 *
 */
public class MmtfSequenceFileReader {

	public static JavaPairRDD<String, StructureDataInterface>  read(String path, JavaSparkContext sc) {
		return sc
				.sequenceFile(path, Text.class, BytesWritable.class)
				.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>,String, StructureDataInterface>() {
					private static final long serialVersionUID = 3512575873287789314L;

					public Tuple2<String, StructureDataInterface> call(Tuple2<Text, BytesWritable> t) throws Exception {
						byte[] values = t._2.copyBytes();
						try {
						    values = ReaderUtils.deflateGzip(t._2.copyBytes()); // unzip binary MessagePack data
						} catch (ZipException e) {
						}
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); // deserialize message pack
						return new Tuple2<String, StructureDataInterface>(t._1.toString(), new GenericDecoder(mmtf)); // decode message pack
					}
				});
	}
	
	public static JavaPairRDD<String, StructureDataInterface>  read(String path, List<String> pdbIds, JavaSparkContext sc) {
		Set<String> pdbIdSet = new HashSet<String>(pdbIds);
		return sc
				.sequenceFile(path, Text.class, BytesWritable.class)
				.filter(t -> pdbIdSet.contains(t._1.toString()))
				.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>,String, StructureDataInterface>() {
					private static final long serialVersionUID = 3512575873287789314L;

					public Tuple2<String, StructureDataInterface> call(Tuple2<Text, BytesWritable> t) throws Exception {
						byte[] values = ReaderUtils.deflateGzip(t._2.copyBytes()); // unzip binary MessagePack data
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); // deserialize message pack
						return new Tuple2<String, StructureDataInterface>(t._1.toString(), new GenericDecoder(mmtf)); // decode message pack
					}
				});
	}
	
	public static JavaPairRDD<String, StructureDataInterface>  read(String path, double fraction, long seed, JavaSparkContext sc) {
		return sc
				.sequenceFile(path, Text.class, BytesWritable.class)
				.sample(false, fraction, seed)
				.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>,String, StructureDataInterface>() {
					private static final long serialVersionUID = 3512575873287789314L;

					public Tuple2<String, StructureDataInterface> call(Tuple2<Text, BytesWritable> t) throws Exception {
						byte[] values = ReaderUtils.deflateGzip(t._2.copyBytes()); // unzip binary MessagePack data
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); // deserialize message pack
						return new Tuple2<String, StructureDataInterface>(t._1.toString(), new GenericDecoder(mmtf)); // decode message pack
					}
				});
	}
}
