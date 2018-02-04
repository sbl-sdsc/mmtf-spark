package edu.sdsc.mmtf.spark.io;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import org.apache.commons.math3.linear.IllConditionedOperatorException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.AminoAcid;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.PDBHeader;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureTools;
import org.biojava.nbio.structure.io.FileParsingParameters;
import org.biojava.nbio.structure.io.MMCIFFileReader;
import org.biojava.nbio.structure.io.PDBFileParser;
import org.biojava.nbio.structure.io.PDBFileReader;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureWriter;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.GenericDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.encoder.AdapterToStructureData;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Methods for reading macromolecular structure in MMTF, mmCIF, and PDB file formats. 
 * The data are returned as a JavaPairRDD with the structure id (e.g. PDB ID) as 
 * the key and the structural data as the value.
 * 
 * <p> Supported operations and file formats:
 * <p><ul>
 * <li> read directory of MMTF-Hadoop Sequence files in full and reduced representation
 * <li> download MMTF full and reduced representations using web services (mmtf.rcsb.org)
 * <li> read directory of MMTF files (.mmtf, mmtf.gz)
 * <li> read directory of mmCIF files (.cif, .cif.gz)
 * <li> read directory of PDB files (.pdb, .ent)
 * </ul>
 * 
 * @author Peter Rose
 * @author Yue Yu
 * @since 0.1.0
 *
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
     * Reads uncompressed PDB files recursively from a given directory path. 
     * This methods reads files with the .pdb or .ent extension.
	 * 
	 * Missing data: bond info, bioAssembly info
	 * Different data : atom serial number, entity description
	 * 
	 * @param path Path to PDB files
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> readPdbFiles(String path, JavaSparkContext sc) {
		return sc
				.parallelize(getFiles(path))
				.mapToPair(new PairFunction<File,String, StructureDataInterface>() {

					private static final long serialVersionUID = -5612212642803414037L;

					public Tuple2<String, StructureDataInterface> call(File f) throws Exception {
						try{
							if(f.toString().contains(".pdb"))
							{
								PDBFileReader pdbreader = new PDBFileReader();
								Structure struc = pdbreader.getStructure(f.toString()); 
								AdapterToStructureData writerToEncoder = new AdapterToStructureData();
								new MmtfStructureWriter(struc, writerToEncoder);
								return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".pdb")), writerToEncoder);
							}
							else if(f.toString().contains(".ent"))
							{
								PDBFileReader pdbreader = new PDBFileReader();
								Structure struc = pdbreader.getStructure(f.toString()); 
								AdapterToStructureData writerToEncoder = new AdapterToStructureData();
								new MmtfStructureWriter(struc, writerToEncoder);
								return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".ent")), writerToEncoder);
							}
							else return null;
						}catch(Exception e)
						{
							return null;
						}
					}
				})
				.filter(t -> t != null);
	}
	
	/**
     * Reads uncompressed Rosetta-style PDB files recursively from a given directory path. 
     * This methods reads files with the .pdb.
     * 
     * Missing data: bond info, bioAssembly info
     * Different data : atom serial number, entity description
     * 
     * @param path Path to PDB files
     * @param sc Spark context
     * @return structure data as keyword/value pairs
     */
    public static JavaPairRDD<String, StructureDataInterface> readRosettaPdbFiles(String path, JavaSparkContext sc) {
        return sc
                .parallelize(getFiles(path))
                .mapToPair(new PairFunction<File,String, StructureDataInterface>() {
                    private static final long serialVersionUID = -7815663658405168429L;

                    public Tuple2<String, StructureDataInterface> call(File f) throws Exception {
                        try{
                            if(f.toString().contains(".pdb"))
                            {
                                PDBFileParser parser = new PDBFileParser();
                                InputStream rosettaStream = rosettaToPdb(f.toString());
                                Structure struc = parser.parsePDBFile(rosettaStream); 
                                rosettaStream.close();
                                // add path as Title
                                PDBHeader header = new PDBHeader();
                                header.setTitle(f.toString());
                                struc.setPDBHeader(header);
                                AdapterToStructureData writerToEncoder = new AdapterToStructureData();
                                new MmtfStructureWriter(struc, writerToEncoder);
                                return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".pdb")), writerToEncoder);
                            }
                            else return null;
                        }catch(Exception e)
                        {
                            return null;
                        }
                    }
                })
                .filter(t -> t != null);
    }
    
    private static InputStream rosettaToPdb(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));

        StringBuilder sb = new StringBuilder();
        List<String> sequence = new ArrayList<String>();
        String line;
        String groupNumber ="";
        
        // extract ATOM records and convert to standard PDB format
        while ((line = br.readLine()) != null) {
            if (line.startsWith("ATOM")) {
                if (line.charAt(21) != 'A') {
                    throw new IllegalArgumentException("ERROR: Multi-chain Rosetta PDB files not supported, yet");
                }
                if (! line.substring(22,26).equals(groupNumber)) {
                    sequence.add(line.substring(17,20));
                    groupNumber = line.substring(22,26);
                }
                sb.append(fixRosettaPdb(line) + "\n");
            }
        }
        
        br.close();
        
        // prepend SEQRES record
        sb = createSeqresRecord(sequence).append(sb);

        return new ByteArrayInputStream(sb.toString().getBytes());
    }
    
    /**
     * Creates a standard PDB SEQRES record from a list of residue names.
     * <p> Format:
     * SEQRES   1 A  159  ASP PRO SER LYS ASP SER LYS ALA GLN VAL SER ALA ALA
     * @param sequence list of 3-character residue names
     * @return SEQRES record
     */
    private static StringBuilder createSeqresRecord(List<String> sequence) {
        StringBuilder builder = new StringBuilder();
        int nRecords = (sequence.size()+12)/13;
        for (int i = 0; i < nRecords; i++) {
            builder.append("SEQRES");
            builder.append(String.format("%4d", i));
            builder.append(" A");
            builder.append(String.format("%5d", sequence.size()));
            int start = i* 13;
            int end = Math.min((i+1) * 13, sequence.size());
            for (int j = start; j < end; j++) {
                builder.append(" ");
                builder.append(sequence.get(j));
            }
            builder.append("\n");
        }
        return builder;
    }

    /**
     * Moves "wrapped" digit in atom names from the first to the last position,
     * e.g., ATOM     47 2HD2 ASN -> ATOM     47 2HD2 ASN
     * @param line
     * @return
     */
    private static String fixRosettaPdb(String line) {
        // wrapped atom names have a digit at position 12
        char line12 = line.charAt(12);
        if (Character.isDigit(line12)) {
            StringBuilder sb = new StringBuilder(line);

            if (line.charAt(14) == ' ') {
                // case 1: "ATOM      8 1H   VAL..." -> "ATOM      8  H1  VAL..."
                sb.setCharAt(12, ' ');
                sb.setCharAt(14, line12);           
            } else if (line.charAt(15) == ' ') {
                // case 2: "ATOM     30 1HB  GLU..." -> "ATOM     30  HB1 GLU..."
                sb.setCharAt(12, ' ');
                sb.setCharAt(15, line12);
            } else if (line.charAt(15) != ' ') {
                // case 3: "ATOM     46 1HD2 ASN..." -> "ATOM     46 HD21 ASN..."
                sb.deleteCharAt(12);
                sb.insert(15, line12);
            }

            line = sb.toString();
        }
        return line;
    }
    
	/**
     * Reads uncompressed and compressed mmCIF files recursively from a given directory path. 
     * This methods reads files with the .cif or .cif.gz extension.
	 * 
	 * 
	 * @param path Path to .cif files
	 * @param sc Spark context
	 * @return structure data as keyword/value pairs
	 */
	public static JavaPairRDD<String, StructureDataInterface> readMmcifFiles(String path, JavaSparkContext sc) {
		return sc
				.parallelize(getFiles(path))
				.mapToPair(new PairFunction<File,String, StructureDataInterface>() {

					private static final long serialVersionUID = -7815663658405168429L;

					public Tuple2<String, StructureDataInterface> call(File f) throws Exception {
						try{
							if(f.toString().contains(".cif.gz"))
							{
								InputStream in = new FileInputStream(f);
								MMCIFFileReader mmcifReader = new MMCIFFileReader();
								Structure struc = mmcifReader.getStructure(new GZIPInputStream(in)); 
								AdapterToStructureData writerToEncoder = new AdapterToStructureData();
								new MmtfStructureWriter(struc, writerToEncoder);
								return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".cif")), writerToEncoder);
							}
							else if(f.toString().contains(".cif"))
							{
								InputStream in = new FileInputStream(f);
								MMCIFFileReader mmcifReader = new MMCIFFileReader();
								Structure struc = mmcifReader.getStructure(in); 
								AdapterToStructureData writerToEncoder = new AdapterToStructureData();
								new MmtfStructureWriter(struc, writerToEncoder);
								return new Tuple2<String, StructureDataInterface>(f.getName().substring(0, f.getName().indexOf(".cif")), writerToEncoder);
							}
							else return null;
						}catch(Exception e)
						{
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
