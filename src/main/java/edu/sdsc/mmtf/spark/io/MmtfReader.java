package edu.sdsc.mmtf.spark.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
 * Methods for reading MMTF Hadoop sequence files and downloading and of individual MMTF files
 * using MMTF web services <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>. 
 * The data are returned as JavaPairRDD with the structure id (e.g. PDB ID) as the key and 
 * the structural data as the value.
 * 
 * @author Peter Rose
 *
 */
public class MmtfReader {

	// TODO read local mmtf and mmtf.gz files
	
	/**
	 * Reads an MMTF Hadoop Sequence file.
	 * See <a href="http://mmtf.rcsb.org/download.html"> for file download information</a>
	 * 
	 * @param path Path to Hadoop sequence file
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> readSequenceFile(String path, JavaSparkContext sc) {
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
	
	/**
	 * Reads the specified PDB entries from a Hadoop Sequence file.
	 * 
	 * @param path Path to Hadoop sequence file
	 * @param pdbIds List of PDB IDs (upper case)
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface>  readSequenceFile(String path, List<String> pdbIds, JavaSparkContext sc) {
		Set<String> pdbIdSet = new HashSet<String>(pdbIds);
		return sc
				.sequenceFile(path, Text.class, BytesWritable.class)
				.filter(t -> pdbIdSet.contains(t._1.toString()))
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
	
	/**
	 * Reads the specified fraction [0,1] of randomly selected PDB entries from a Hadoop Sequence file.
	 * 
	 * @param path Path to Hadoop sequence file
	 * @param fraction Fraction of entries to be read [0,1]
	 * @param seed Seed for random number generator
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> readSequenceFile(String path, double fraction, long seed, JavaSparkContext sc) {
		return sc
				.sequenceFile(path, Text.class, BytesWritable.class)
				.sample(false, fraction, seed)
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
	
	/**
	 * Downloads and reads the specified PDB entries using <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>.
	 * 
	 * @param pdbIds List of PDB IDs (upper case)
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> downloadMmtfFiles(List<String> pdbIds, JavaSparkContext sc) {
		return sc
				.parallelize(pdbIds)
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t, getStructure(t, false, false)));
	}
	
	/**
	 * Downloads and reads the specified PDB entries using <a href="http://mmtf.rcsb.org/download.html">MMTF web services</a>.
	 * 
	 * @param pdbIds List of PDB IDs (upper case)
	 * @param https if true, used https instead of http
	 * @param reduced if true, downloads a reduced representation (C-alpha, P-backbone, all ligand atoms)
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> downloadMmtfFiles(List<String> pdbIds, boolean https, boolean reduced, JavaSparkContext sc) {
		return sc
				.parallelize(pdbIds)
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t, getStructure(t, https, reduced)));
	}
	
	private static StructureDataInterface getStructure(String pdbId, boolean https, boolean reduced) throws IOException {
// TODO use with new version		return new GenericDecoder(ReaderUtils.getDataFromUrl(pdbId, https, reduced));
		return new GenericDecoder(ReaderUtils.getDataFromUrl(pdbId));
	}

}
