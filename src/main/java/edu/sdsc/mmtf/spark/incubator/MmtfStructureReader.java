package edu.sdsc.mmtf.spark.incubator;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Reads and decodes an MMTF Hadoop Sequence file. It returns a JavaPairRDD with the structure id
 * (e.g. PDB ID) as the key and the MMTF StructureDataInterface as the value.
 * @author Peter Rose
 *
 */
public class MmtfStructureReader {

	public static JavaPairRDD<String, MmtfStructure>  read(String path, JavaSparkContext sc) {
		return sc
				.sequenceFile(path, Text.class, BytesWritable.class)
				.mapToPair(new PairFunction<Tuple2<Text, BytesWritable>,String, MmtfStructure>() {
					private static final long serialVersionUID = 3512575873287789314L;

					public Tuple2<String, MmtfStructure> call(Tuple2<Text, BytesWritable> t) throws Exception {
						byte[] values = ReaderUtils.deflateGzip(t._2.copyBytes()); // unzip binary MessagePack data
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); // deserialize message pack
						return new Tuple2<String, MmtfStructure>(t._1.toString(), mmtf); // decode message pack
					}
				});
	}
}
