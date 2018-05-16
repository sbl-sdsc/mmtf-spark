package edu.sdsc.mmtf.spark.datasets;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.upper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class maps human genetic variation positions to PDB structure positions. 
 * Genomic positions must be specified for the hgvs-grch37 reference genome using the 
 * <a href="http://varnomen.hgvs.org/">HGVS sequence variant nomenclature</a>.
 * <p>
 * Example:
 * <pre>
 * {@code
 * List<String> variantIds = Arrays.asList("chr7:g.140449098T>C", "chr7:g.140449100T>C");
 * Dataset<Row> ds = G2SDataset.getPositionDataset(variantIds, "3TV4", "A");
 * ds.show()
 * +-----------+-------+-----------+------------+-----------+-------------------+
 * |structureId|chainId|pdbPosition|pdbAminoAcid|  refGenome|        variationId|
 * +-----------+-------+-----------+------------+-----------+-------------------+
 * |       3TV4|      A|        661|           N|hgvs-grch37|chr7:g.140449098T>C|
 * |       3TV4|      A|        660|           N|hgvs-grch37|chr7:g.140449100T>C|
 * +-----------+-------+-----------+------------+-----------+-------------------+
 * }
 * </pre>
 * 
 * <p>
 * See <a href="https://g2s.genomenexus.org/">G2S Web Services</a>.
 * 
 * <p>
 * References:
 * <p>
 * Juexin Wang, Robert Sheridan, S Onur Sumer, Nikolaus Schultz, Dong Xu,
 * Jianjiong Gao; G2S: a web-service for annotating genomic variants on 3D 
 * protein structures (2018) Bioinformatics,
 * <a href="https://doi.org/10.1093/bioinformatics/bty047">doi:10.1093/bioinformatics/bty047</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class G2SDataset {
    private static final String REFERENCE_GENOME = "hgvs-grch37";
    private static final String G2S_REST_URL = "https://g2s.genomenexus.org/api/alignments/"+REFERENCE_GENOME+"/";

    /**
     * Downloads PDB residue mappings for a list of genomic variations.
     * @param variationIds genomic variation ids (e.g., chr7:g.140449103A>C)
     * @return dataset with PDB mapping information
     * @throws IOException
     */
    public static Dataset<Row> getPositionDataset(List<String> variationIds) throws IOException {
        return getPositionDataset(variationIds, null, null);
    }
    
    /**
     * Downloads PDB residue mappings for a list of genomic variations.
     * @param variationIds genomic variation ids (e.g., chr7:g.140449103A>C)
     * @param structureId specific PDB structure used for mapping
     * @param chainId specific chain used for mapping
     * @return dataset with PDB mapping information
     * @throws IOException
     */
    public static Dataset<Row> getPositionDataset(List<String> variationIds , String structureId, String chainId) throws IOException {
        Dataset<Row> dataset = getDataset(variationIds, structureId, chainId);
        if (dataset == null) return null;

        return dataset
                .select("structureId","chainId","pdbPosition","pdbAminoAcid","refGenome","variationId")
                .distinct();
    }
    
    /**
     * Downloads PDB residue mappings and alignment information for a list of genomic variations.
     * @param variationIds genomic variation ids (e.g., chr7:g.140449103A>C)
     * @return dataset with PDB mapping information
     * @throws IOException
     */
    public static Dataset<Row> getFullDataset(List<String> variationIds) throws IOException {
        return getFullDataset(variationIds, null, null);
    }
    
    /**
     * Downloads PDB residue mappings and alignment information for a list of genomic variations.
     * @param variationIds genomic variation ids (e.g., chr7:g.140449103A>C)
     * @param structureId specific PDB structure used for mapping
     * @param chainId specific chain used for mapping
     * @return dataset with PDB mapping information
     * @throws IOException
     */
    public static Dataset<Row> getFullDataset(List<String> variationIds, String structureId, String chainId) throws IOException {
        return getDataset(variationIds, structureId, chainId);
    }
    
    /**
     * Downloads PDB residue mappings for a list of genomic variations.
     * @param variationIds genomic variation ids (e.g. chr7:g.140449103A>C)
     * @param pdbId specific PDB structure used for mapping
     * @param chainId specific chain used for mapping
     * @return dataset with PDB mapping information
     * @throws IOException
     */
    private static Dataset<Row> getDataset(List<String> variationIds, String structureId, String chainId) throws IOException {
        // get a spark context
        SparkSession spark = SparkSession.builder().getOrCreate();    
        @SuppressWarnings("resource") // sc will be closed elsewhere
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // download data in parallel
        JavaRDD<String> data = sc.parallelize(variationIds).flatMap(m -> getData(m, structureId, chainId));

        // convert from JavaRDD to Dataset
        Dataset<String> jsonData = spark.createDataset(JavaRDD.toRDD(data), Encoders.STRING()); 

        // parse json strings and return as a dataset
        Dataset<Row> dataset = spark.read().json(jsonData); 
        dataset.show();
        
        // return null if dataset is empty
        if (dataset.columns().length == 0) {
            System.out.println("G2SDataset: no matches found");
            return null;
        }   
           
        dataset = standardizeData(dataset);
        
        return flattenDataset(dataset);
    }

    private static Iterator<String> getData(String variationId, String structureId, String chainId) throws IOException {
        List<String> data = new ArrayList<>();

        // open input stream
        InputStream is = null;
        URL u = null;
        if (structureId == null) {
            u = new URL(G2S_REST_URL + variationId + "/residueMapping");
        } else {
            u = new URL(G2S_REST_URL + variationId + "/pdb/" + structureId + "_" + chainId + "/residueMapping");
        }

        try {
            is = u.openStream();
        } catch (IOException e) {
        	e.printStackTrace();
            System.err.println("WARNING: Could not load data for: " + variationId);
            return data.iterator();
        }

        // read data
        List<String> results = IOUtils.readLines(is, "UTF-8");

        // annotate and save data
        if (results != null &&  results.get(0).length() > 100) {
            addVariantId(results, REFERENCE_GENOME, variationId);
            data.addAll(results);
        }

        is.close();

        return data.iterator();
    }

    /**
     * Adds reference genome and variationId to each json record.
     * 
     * <pre>
     * Example:
     * replace
     *   {"alignmentId":1902156,...
     * with
     *   {"refGenome":"hgvs-grch37","variationId":"chr7:g.140449098T>C","alignmentId":1902156,...
     * </pre>
     * @param json list of original json strings
     * @param refGenome reference genome
     * @param variationId variation identifier
     */
    private static void addVariantId(List<String> json, String refGenome, String variationId) {
        for (int i = 0; i < json.size(); i++) {
            String ids = "\"refGenome\":\"" + refGenome + "\"," + "\"variationId\":\"" + variationId + "\"," + "\"alignmentId\"";
            String augmentedRecord = json.get(i).replaceAll("\"alignmentId\"", ids);
            json.set(i, augmentedRecord);   
        }  
    }
    
    /**
     * Standardized data and column names to be consistent
     * with other datasets.
     * 
     * @param ds
     * @return
     */
    private static Dataset<Row> standardizeData(Dataset<Row> ds) {
        return ds.withColumn("structureId", upper(col("pdbId")))
                 .withColumnRenamed("chain", "chainId");
    }
    
    private static Dataset<Row> flattenDataset(Dataset<Row> ds) {
        return ds.withColumn("pdbPosition", explode(col("residueMapping.pdbPosition")))
                 .withColumn("pdbAminoAcid", explode(col("residueMapping.pdbAminoAcid")));
    }
}