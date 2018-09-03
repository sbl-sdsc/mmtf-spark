package edu.sdsc.mmtf.spark.datasets;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.upper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * This class downloads PDB to UniProt chain and residue-level mappings
 * from the SIFTS project.
 * Data are retrieved from a data cache and only data that are not cached 
 * are downloaded when required.
 * This class also provides a method to create a new mapping file.
 * 
 * For more information about SIFTS see:
 * <p>
 * The "Structure Integration with Function, Taxonomy and Sequence"
 * (<a href="https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html">SIFTS</a>) is
 * the authoritative source of up-to-date chain and residue-level
 * mappings to UniProt.
 *
 *<p> Example of chain-level mappings
 * <pre>
 * +----------------+-----------+-------+---------+
 * |structureChainId|structureId|chainId|uniprotId|
 * +----------------+-----------+-------+---------+
 * |          1A02.F|       1A02|      F|   P01100|
 * |          1A02.J|       1A02|      J|   P05412|
 * |          1A02.N|       1A02|      N|   Q13469|
 * </pre>
 * 
 * <p> Example of residue-level mappings
 * <pre>
 * Columns:
 * structureChainId - pdbId.chainId
 * pdbResNum - PDB residue number in ATOM records
 * pdbSeqNum - PDB residue number in the sequence record (index start at 1)
 * uniprotId - UniProt id (accession number)
 * uniprotNum - UniProt residue number (index starts at 1)
 * 
 * +----------------+---------+---------+---------+----------+
 * |structureChainId|pdbResNum|pdbSeqNum|uniprotId|uniprotNum|
 * +----------------+---------+---------+---------+----------+
 * |          1STP.A|     null|        1|   P22629|        25|
 * |          1STP.A|     null|        2|   P22629|        26|
 * |          1STP.A|     null|        3|   P22629|        27|
 * |          1STP.A|     null|       12|   P22629|        36|
 * ...
 * |          1STP.A|       13|       13|   P22629|        37|
 * |          1STP.A|       14|       14|   P22629|        38|
 * |          1STP.A|       15|       15|   P22629|        39|
 * ...
 *</pre>
 * 
 * @author Peter Rose
 * @since 0.2.0
 */
public class PdbToUniProt {
    // location of cached dataset (temporary location until we find a better site to host these data)
//    private static final String CACHED_FILE_URL = "https://github.com/sbl-sdsc/sifts-columnar/raw/master/data/pdb2uniprot_residues.orc.lzo";
    private static final String CACHED_FILE_URL = "https://github.com/sbl-sdsc/mmtf-data/raw/master/data/pdb2uniprot_residues.orc.lzo";
    private static final String FILENAME = "pdb2uniprot_residues.orc.lzo";
    // location of SIFTS data
    private static final String UNIPROT_MAPPING_URL = "http://ftp.ebi.ac.uk/pub/databases/msd/sifts/flatfiles/csv/pdb_chain_uniprot.csv.gz";
    private static final String UNIPROT_FILE = "pdb_chain_uniprot.csv.gz";
    private static final String SIFTS_URL = "http://ftp.ebi.ac.uk/pub/databases/msd/sifts/split_xml/";
  
    /**
     * Returns an up-to-date dataset of PDB to UniProt
     * chain-level mappings using SIFTS data.
     * 
     * <p> Example:
     * <pre>
     * +----------------+-----------+-------+---------+
     * |structureChainId|structureId|chainId|uniprotId|
     * +----------------+-----------+-------+---------+
     * |          1A02.F|       1A02|      F|   P01100|
     * |          1A02.J|       1A02|      J|   P05412|
     * |          1A02.N|       1A02|      N|   Q13469|
     * 
     * @return dataset of PDB to UniProt chain-level mappings
     * @throws IOException
     */
    public static Dataset<Row> getChainMappings() throws IOException {
        
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.sparkContext().addFile(UNIPROT_MAPPING_URL);

        // parse csv file
        Dataset<Row> dataset = spark.read()
                .option("header", "true")
                .option("comment", "#")
                .option("inferSchema", "true")
                .csv(SparkFiles.get(UNIPROT_FILE));

        // cleanup and rename columns to be consistent with MMTF conventions.
        dataset = dataset.withColumn("PDB", upper(col("PDB")))
                         .withColumnRenamed("PDB","structureId")
                         .withColumnRenamed("CHAIN","chainId")
                         .withColumnRenamed("SP_PRIMARY","uniprotId")
                         .withColumn("structureChainId", concat_ws(".", col("structureId"), col("chainId")))
                         .select("structureChainId", "structureId", "chainId", "uniprotId");
        
        return dataset;
    }
    
    /**
     * Returns an up-to-date dataset of PDB to UniProt
     * chain-level mappings for a list of ids.
     * Valid ids are either a list of pdbIds (e.g. 1XYZ) or pdbId.chainIds (e.g., 1XYZ.A).
     * 
     * @param ids list of pdbIds or pdbId.chainIds
     * @return dataset of PDB to UniProt chain-level mappings
     * @throws IOException
     */
    public static Dataset<Row> getChainMappings(List<String> ids) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();
        
        // get a dataset of up-to-date UniProt chain mappings
        Dataset<Row> ds = getChainMappings();  
        // create a dataset of ids from the passed-in list
        Dataset<Row> subset = spark.createDataset(ids, Encoders.STRING()).toDF("id");
        
        // create subsets of data
        if (!ids.isEmpty()) {
            if (ids.get(0).length() == 4) {
                // join by pdbId
                ds = ds.join(subset, ds.col("structureId").equalTo(subset.col("id"))).drop("id");    
            } else {
                // join by pdbChainId
                ds = ds.join(subset, ds.col("structureChainId").equalTo(subset.col("id"))).drop("id");    
            }
        }
        
        return ds;
    }
    
    /**
     * Returns an up-to-date dataset of PDB to UniProt 
     * residue-level mappings for a list of ids.
     * Valid ids are either a list of pdbIds (e.g. 1XYZ) or pdbId.chainId (e.g., 1XYZ.A).
     * This method reads a cached file and downloads updates.
     * 
     * @param ids list of pdbIds or pdbId.chainIds
     * @return dataset of PDB to UniProt residue-level mappings
     * @throws IOException
     */
    public static Dataset<Row> getResidueMappings(List<String> ids) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();
        
        boolean withChainId = ids.size() > 0 && ids.get(0).length() > 4;
        
        // create dataset of ids
        Dataset<Row> df = spark.createDataset(ids, Encoders.STRING()).toDF("id");
        // get cached mappings
        Dataset<Row> mapping = getCachedResidueMappings();  
        
        // dataset for non-cached mappings
        Dataset<Row> notCached = null;
        // dataset with PDB Ids to be downloaded
        Dataset<Row> toDownload = null; 
        
        if (withChainId) {
            // get subset of requested ids from cached dataset
            mapping = mapping.join(df, mapping.col("structureChainId").equalTo(df.col("id"))).drop("id");
            // get ids that are not in the cached dataset
            notCached = df.join(mapping, df.col("id").equalTo(mapping.col("structureChainId")), "left_anti").cache(); 
            // create dataset of PDB Ids to be downloaded
            toDownload = notCached.withColumn("id", col("id").substr(0, 4)).distinct().cache();
        } else {
            // get subset of requested ids from cached dataset
            mapping = mapping.withColumn("pdbId", col("structureChainId").substr(0, 4));
            mapping = mapping.join(df, mapping.col("pdbId").equalTo(df.col("id"))).drop("id");
            // create dataset of PDB Ids to be downloaded
            toDownload = df.join(mapping, df.col("id").equalTo(mapping.col("pdbId")), "left_anti").distinct().cache();
            mapping = mapping.drop("pdbId");
        }
        
        toDownload = toDownload.distinct().cache();
            
        // download data that are not in the cache
        if (toDownload.count() > 0) {
            Dataset<Row> unpData = getChainMappings().select("structureId").distinct();
            toDownload = toDownload.join(unpData, toDownload.col("id").equalTo(unpData.col("structureId"))).drop("structureId").cache();
            System.out.println("Downloading mapping for " + toDownload.count() + " PDB structures.");
            Dataset<Row> downloadedData = downloadData(toDownload);
      
            // since data are downloaded for all chains in structure, make sure to only include the requested chains.
            if (withChainId) {
                downloadedData = downloadedData.join(notCached, downloadedData.col("structureChainId").equalTo(notCached.col("id"))).drop("id");
            }
            mapping = mapping.union(downloadedData);
        }
        
        return mapping;
    }
    
    /**
     * Returns an up-to-date dataset of PDB to UniProt residue-level mappings.
     * This method reads a cached file and downloads updates.
     * 
     * <p> Example of residue-level mappings
     * <pre>
     * Columns:
     * structureChainId - pdbId.chainId
     * pdbResNum - PDB residue number in ATOM records
     * pdbSeqNum - PDB residue number in the sequence record (index start at 1)
     * uniprotId - UniProt id (accession number)
     * uniprotNum - UniProt residue number (index starts at 1)
     * 
     * +----------------+---------+---------+---------+----------+
     * |structureChainId|pdbResNum|pdbSeqNum|uniprotId|uniprotNum|
     * +----------------+---------+---------+---------+----------+
     * |          1STP.A|     null|        1|   P22629|        25|
     * |          1STP.A|     null|        2|   P22629|        26|
     * |          1STP.A|     null|        3|   P22629|        27|
     * |          1STP.A|     null|       12|   P22629|        36|
     * ...
     * |          1STP.A|       13|       13|   P22629|        37|
     * |          1STP.A|       14|       14|   P22629|        38|
     * |          1STP.A|       15|       15|   P22629|        39|
     * ...
     *</pre>
     *
     * @return dataset of PDB to UniProt residue-level mappings
     * @throws IOException
     */
    public static Dataset<Row> getResidueMappings() throws IOException {
        List<String> pdbIds = getChainMappings().select("structureId").distinct().as(Encoders.STRING()).collectAsList();
        return PdbToUniProt.getResidueMappings(pdbIds);
    }

    /**
     * Returns the current version of the cached dataset of PDB to UniProt 
     * residue mappings. This method is significantly faster, but may not
     * contain the mapping for recently released PDB entries.
     * 
     * @return dataset of PDB to UniProt residue mappings
     * @throws IOException
     */
    public static Dataset<Row> getCachedResidueMappings() {       
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.orc.impl", "native");
        spark.sparkContext().addFile(CACHED_FILE_URL);

        return spark.read().format("orc").load(SparkFiles.get(FILENAME));
    }

    /**
     * Builds a new PDB to UniProt residue mapping file using data from
     * the SIFTS project. Downloads the mappings for each PDB Id using
     * SIFTS webservices and saves the dataset to a file. 
     * This method may take 1 - 2 days to download data.
     * 
     * @param filename file name for dataset
     * @param fileFormat "parquet" or "orc"
     * @param compressionCodec "gzip" or "snappy" for parquet, "zlib" or "lzo" for orc
     * @throws IOException
     */
    public static void buildDataset(String filename, String fileFormat, String compressionCodec) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.orc.impl", "native");
        
        Dataset<Row> toDownload = getChainMappings().select("structureId").distinct();
        System.out.println("Downloading mapping for " + toDownload.count() + " PDB structures.");
        Dataset<Row> ds = downloadData(toDownload);
        ds.write().mode("overwrite").option("compression", compressionCodec).format(fileFormat).save(filename);
    }
    
    /**
     * Updates a cached dataset of PDB to UniProt residue mapping file using 
     * up-to-date data from the SIFTS project and saves the dataset to a file. 
     * 
     * @param filename file name for dataset
     * @param fileFormat "parquet" or "orc"
     * @param compressionCodec "gzip" or "snappy" for parquet, "zlib" or "lzo" for orc
     * @throws IOException
     */
    public static void updateDataset(String filename, String fileFormat, String compressionCodec) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();
        spark.conf().set("spark.sql.orc.impl", "native");
        
        Dataset<Row> ds = PdbToUniProt.getResidueMappings();
        ds.write().mode("overwrite").option("compression", compressionCodec).format(fileFormat).save(filename);
    }

    /**
     * Downloads PDB to UniProt residue-level mappings.
     * 
     * @param ds dataset with the first column being a PDB Id in upper case.
     * @return dataset with mappings
     * @throws IOException
     */
    public static Dataset<Row> downloadData(Dataset<Row> ds) throws IOException {       
        StructType structType = new StructType();
        structType = structType.add("structureChainId", DataTypes.StringType, false);
        structType = structType.add("pdbResNum", DataTypes.StringType, true);
        structType = structType.add("pdbSeqNum", DataTypes.IntegerType, false);
        structType = structType.add("pdbResName", DataTypes.StringType, true);
        structType = structType.add("uniprotId", DataTypes.StringType, true);
        structType = structType.add("uniprotNum", DataTypes.IntegerType, true);
        structType = structType.add("uniprotName", DataTypes.StringType, true);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> output = ds.flatMap(new FlatMapFunction<Row, Row>() {
            private static final long serialVersionUID = 7569478102816528384L;

            @Override
            public Iterator<Row> call(Row row) throws IOException {
                List<Row> rows = new ArrayList<Row>();
                String pdbId = row.getString(0); // first column must be an upper case PDB ID!!!
                System.out.println(pdbId);

                String id = pdbId.toLowerCase();
                URL u = new URL(SIFTS_URL + id.substring(1, 3) + "/" + id + ".xml.gz");

                BufferedReader rd = null;
                
                // downloads occasionally time out, make 3 trials
                for (int trial = 0; trial < 3; trial++) {
                    try {
                        URLConnection conn = u.openConnection();
                        InputStream in = conn.getInputStream();
                        rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
                    } catch (IOException e) {
                        System.out.println("retrying... " + pdbId);
                        try {
                            Thread.sleep(1000 * trial);
                        } catch (InterruptedException e1) {
                        }
                    }
                    if (rd != null)
                        break;
                }

                Object[] mapping = null;

                // extract info from file
                // TODO replace this code with an xml file parser
                if (rd != null) {
                    String line;
                    try {
                        while ((line = rd.readLine()) != null) {
                            line = line.trim();
                            if (line.startsWith("<residue ")) {
                                mapping = new Object[7];
                                mapping[2] = getIntAttributeValue("dbResNum", line);
                                mapping[3] = getAttributeValue("dbResName", line);
                            } else if (line.startsWith("<crossRefDb dbSource=\"PDB")) {
                                mapping[0] = getAttributeValue("dbAccessionId", line).toUpperCase() + "." + getAttributeValue("dbChainId", line);
                                mapping[1] = getAttributeValue("dbResNum", line);
                            } else if (line.startsWith("<crossRefDb dbSource=\"UniProt")) {
                                mapping[4] = getAttributeValue("dbAccessionId", line);
                                mapping[5] = getIntAttributeValue("dbResNum", line);
                                mapping[6] = getAttributeValue("dbResName", line);
                            }
                            if (line.equals("</residue>")) {
                                if (mapping[0] != null) {
                                    rows.add(RowFactory.create((Object[]) mapping));
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("problem reading " + pdbId + " ... skipping.");
                    }

                    rd.close();
                }
            
                return rows.iterator();
            }
        }, encoder);
        
        return output;
    }
    
    /**
     * Reads PDB to UniProt residue-level mappings from local file system.
     * 
     * 
     * @param ds dataset with the first column being a PDB Id in upper case.
     * @path path to a local copy of the split_xml directory rsynced from EBI.
     * @return dataset with mappings
     * @throws IOException
     */
    public static Dataset<Row> readData(Dataset<Row> ds, String path) throws IOException {       
        StructType structType = new StructType();
        structType = structType.add("structureChainId", DataTypes.StringType, false);
        structType = structType.add("pdbResNum", DataTypes.StringType, true);
        structType = structType.add("pdbSeqNum", DataTypes.IntegerType, false);
        structType = structType.add("pdbResName", DataTypes.StringType, true);
        structType = structType.add("uniprotId", DataTypes.StringType, true);
        structType = structType.add("uniprotNum", DataTypes.IntegerType, true);
        structType = structType.add("uniprotName", DataTypes.StringType, true);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

        Dataset<Row> output = ds.flatMap(new FlatMapFunction<Row, Row>() {
            private static final long serialVersionUID = 7569478102816528384L;

            @Override
            public Iterator<Row> call(Row row) throws IOException {
                List<Row> rows = new ArrayList<Row>();
                String pdbId = row.getString(0); // first column must be an upper case PDB ID!!!
                System.out.println(pdbId);

                String id = pdbId.toLowerCase();
                String fileName = path + "/" +  id.substring(1, 3) + "/" + id + ".xml.gz";
                if (path.endsWith("/")) {
                    fileName = path + id.substring(1, 3) + "/" + id + ".xml.gz";
                }

                BufferedReader rd = null;
                
                // downloads occasionally time out, make 3 trials
                for (int trial = 0; trial < 3; trial++) {
                    try {
                        InputStream in = new FileInputStream(fileName);
                        rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
                    } catch (IOException e) {
                        System.out.println("retrying... " + pdbId);
                        try {
                            Thread.sleep(1000 * trial);
                        } catch (InterruptedException e1) {
                        }
                    }
                    if (rd != null)
                        break;
                }

                Object[] mapping = null;

                // extract info from file
                // TODO replace this code with an xml file parser
                if (rd != null) {
                    String line;
                    try {
                        while ((line = rd.readLine()) != null) {
                            line = line.trim();
                            if (line.startsWith("<residue ")) {
                                mapping = new Object[7];
                                mapping[2] = getIntAttributeValue("dbResNum", line);
                                mapping[3] = getAttributeValue("dbResName", line);
                            } else if (line.startsWith("<crossRefDb dbSource=\"PDB")) {
                                mapping[0] = getAttributeValue("dbAccessionId", line).toUpperCase() + "." + getAttributeValue("dbChainId", line);
                                mapping[1] = getAttributeValue("dbResNum", line);
                            } else if (line.startsWith("<crossRefDb dbSource=\"UniProt")) {
                                mapping[4] = getAttributeValue("dbAccessionId", line);
                                mapping[5] = getIntAttributeValue("dbResNum", line);
                                mapping[6] = getAttributeValue("dbResName", line);
                            }
                            if (line.equals("</residue>")) {
                                if (mapping[0] != null) {
                                    rows.add(RowFactory.create((Object[]) mapping));
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.out.println("problem reading " + pdbId + " ... skipping.");
                    }

                    rd.close();
                }
            
                return rows.iterator();
            }
        }, encoder);
        
        return output;
    }
    /**
     * Retrieve an XML integer attribute from a line of XML.
     * @param attribute
     * @param line
     * @return
     */
    private static Integer getIntAttributeValue(String attribute, String line) {
        String value =  getAttributeValue(attribute, line);
        return Integer.parseInt(value);
    }
    
    /**
     * Retrieve an XML attribute from a line of XML.
     * @param attribute
     * @param line
     * @return
     */
    private static String getAttributeValue(String attribute, String line) {
        int start = line.indexOf(attribute + "=\"") + attribute.length() + 2;
        if (start < 1) {
            return null;
        }
        String value = line.substring(start);
        int end = value.indexOf("\"");
        if (end < 1) {
            return null;
        }
        value = value.substring(0, end);
        if (value.equals("null")) {
            return null;
        }
        return value;
    }
}