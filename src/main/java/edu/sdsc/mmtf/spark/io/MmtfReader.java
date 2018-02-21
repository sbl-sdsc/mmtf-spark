package edu.sdsc.mmtf.spark.io;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
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
 * Methods for reading and downloading macromolecular structures in MMTF file formats. 
 * The data are returned as a JavaPairRDD with the structure id (e.g. PDB ID) as 
 * the key and the structural data as the value.
 * 
 * <p> Supported operations and file formats:
 * <p><ul>
 * <li> read directory of MMTF-Hadoop Sequence files in full and reduced representation
 * <li> download MMTF full and reduced representations using web services (mmtf.rcsb.org)
 * <li> read directory of MMTF files (.mmtf, mmtf.gz)
 * </ul>
 * 
 * @author Peter Rose
 * @author Yue Yu
 * @since 0.1.0
 * @see   MmtfImporter
 */
public class MmtfReader {

	/**
	 * Reads a full MMTF-Hadoop Sequence file using the default file location.
	 * The default file location is determined by {@link MmtfReader.getMmtfFullPath()}.
	 * 
	 * See <a href="https://mmtf.rcsb.org/download.html"> for file download information</a>
	 * 
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 * @throws FileNotFoundException 
	 */
	public static JavaPairRDD<String, StructureDataInterface> readFullSequenceFile(JavaSparkContext sc) throws FileNotFoundException {
		return readSequenceFile(getMmtfFullPath(), sc);
	}
	
	/**
	 * Reads a reduced MMTF-Hadoop Sequence file using the default file location.
	 * The default file location is determined by {@link MmtfReader.getMmtfReducedPath()}.
	 * 
	 * See <a href="https://mmtf.rcsb.org/download.html"> for file download information</a>
	 * 
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 * @throws FileNotFoundException 
	 */
	public static JavaPairRDD<String, StructureDataInterface> readReducedSequenceFile(JavaSparkContext sc) throws FileNotFoundException {
		return readSequenceFile(getMmtfReducedPath(), sc);
	}
	
	/**
	 * Reads an MMTF-Hadoop Sequence file. The Hadoop Sequence file may contain
	 * either gzip compressed or uncompressed values.
	 * See <a href="https://mmtf.rcsb.org/download.html"> for file download information</a>
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
						
						// if data are gzipped, unzip them first
						try {
						    values = ReaderUtils.deflateGzip(t._2.copyBytes());
						} catch (ZipException e) {}
						
						// deserialize message pack
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); 
						
						// decode message pack
						return new Tuple2<String, StructureDataInterface>(t._1.toString(), new GenericDecoder(mmtf)); 
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
						// if data are gzipped, unzip them first
						try {
						    values = ReaderUtils.deflateGzip(t._2.copyBytes()); // unzip binary MessagePack data
						} catch (ZipException e) {}
						
						// deserialize message pack
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); // deserialize message pack
						
						// decode message pack
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
						// if data are gzipped, unzip them first
						try {
						    values = ReaderUtils.deflateGzip(t._2.copyBytes()); // unzip binary MessagePack data
						} catch (ZipException e) {}
						
						// deserialize message pack
						MmtfStructure mmtf = new MessagePackSerialization().deserialize(new ByteArrayInputStream(values)); // deserialize message pack
						
						// decode message pack
						return new Tuple2<String, StructureDataInterface>(t._1.toString(), new GenericDecoder(mmtf)); // decode message pack
					}
				});
	}
	
	

	/**
	 * Get list of files from the path
	 * 
	 * @param path File path
	 * @return list of files in the path
	 */
	private static List<File> getFiles(String path)
	{
		List<File> fileList = new ArrayList<File>();
		for(File f: new File(path).listFiles())
		{
			if(f.isDirectory()) fileList.addAll(getFiles(f.toString()));
			else fileList.add(f);
		}
		return fileList;
	}
		
	
	/**
	 * Reads uncompressed and compressed MMTF files recursively from
	 * a given directory. 
	 * This methods reads files with the mmtf or mmtf.gz extension.
	 * 
	 * @param path Path to MMTF files
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> readMmtfFiles(String path, JavaSparkContext sc) {
		return sc
				.parallelize(getFiles(path))
				.mapToPair(new PairFunction<File,String, StructureDataInterface>() {
					private static final long serialVersionUID = 9018971417443154996L;

					public Tuple2<String, StructureDataInterface> call(File f) throws Exception {
						try{
							if(f.toString().contains(".mmtf.gz"))
							{
								InputStream in = new FileInputStream(f);
								MmtfStructure mmtf = new MessagePackSerialization().deserialize(new GZIPInputStream(in));
								return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".mmtf")), new GenericDecoder(mmtf));
							}
							else if(f.toString().contains(".mmtf"))
							{
								InputStream in = new FileInputStream(f);
								MmtfStructure mmtf = new MessagePackSerialization().deserialize(in); 
								return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".mmtf")), new GenericDecoder(mmtf));					
							}
							else return null;
						}catch(Exception e)
						{
							System.out.println(e);
							return null;
						}
					}
				})
				.filter(t -> t != null);
	}
	
	/**
	 * Downloads and reads the specified PDB entries in the full MMTF representation
	 * using MMTF web services.
	 * 
	 * @param pdbIds List of PDB IDs (upper case)
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 * @see <a href="https://mmtf.rcsb.org/download.html">MMTF web services</a>.
	 */
	public static JavaPairRDD<String, StructureDataInterface> downloadFullMmtfFiles(List<String> pdbIds, JavaSparkContext sc) {
		return sc
				.parallelize(pdbIds)
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t, getStructure(t, true, false)));
	}
	
	/**
	 * Downloads and reads the specified PDB entries in the reduced MMTF representation (C-alpha, P-backbone, all ligand atoms)
	 * using MMTF web services.
	 * 
	 * @param pdbIds List of PDB IDs (upper case)
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 * @see <a href="https://mmtf.rcsb.org/download.html">MMTF web services</a>.
	 */
	public static JavaPairRDD<String, StructureDataInterface> downloadReducedMmtfFiles(List<String> pdbIds, JavaSparkContext sc) {
		return sc
				.parallelize(pdbIds)
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t, getStructure(t, true, true)));
	}
	
	private static StructureDataInterface getStructure(String pdbId, boolean https, boolean reduced) throws IOException {		
	    return new GenericDecoder(ReaderUtils.getDataFromUrl(pdbId, https, reduced));
	}

	/**
	 * Returns the path to the full MMTF-Hadoop sequence file.
	 * It first check if the system property MMTF_FULL has been set
	 * (with JVM -DMMTF_FULL=<path> option). If the system property
	 * is not set, it looks for the environment variable MMTF_FULL.
	 * 
	 * @return path to full MMTF-Hadoop file if set
	 * @throws FileNotFoundException if path has not been set
	 * @since 0.2.0
	 */
    public static String getMmtfFullPath() throws FileNotFoundException {
        // get path from System property (set with -DMMTF_FULL=<path>)
        String path = System.getProperty("MMTF_FULL");
        if (path == null) {
            // get path from environment variable
            path = System.getenv("MMTF_FULL");
            if (path == null) {
                throw new FileNotFoundException("Path to full MMTF-Hadoop file not set.");
            }
        } 
        System.out.println("Hadoop Sequence file path: MMTF_FULL=" + path);
        return path;
    }
    /**
     * Returns the path to the reduced MMTF-Hadoop sequence file.
     * It first check if the system property MMTF_REDUCED has been set
     * (with JVM -DMMTF_REDUCED=<path> option). If the system property
     * is not set, it looks for the environment variable MMTF_REDUCED.
     * 
     * @return path to reduced MMTF-Hadoop file
     * @throws FileNotFoundException if path has not been set
     * @since 0.2.0
     */
    public static String getMmtfReducedPath() throws FileNotFoundException {
        // get path from System property (set with -DMMTF_REDUCED=<path>)
        String path = System.getProperty("MMTF_REDUCED");
        if (path == null) {
            // get path from environment variable
            path = System.getenv("MMTF_REDUCED");
            if (path == null) {
                throw new FileNotFoundException("Path to reduced MMTF-Hadoop file not set.");
            }
        }
        System.out.println("Hadoop Sequence file path: MMTF_REDUCED=" + path);
        return path;
    }
}
