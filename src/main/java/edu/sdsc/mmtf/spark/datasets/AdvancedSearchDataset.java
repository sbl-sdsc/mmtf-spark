package edu.sdsc.mmtf.spark.datasets;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.substring_index;

import java.io.IOException;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.webservices.AdvancedQueryService;

/**
 * Runs an RCSB PDB Advanced Search web service using an XML query description.
 * See <a href="https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedSearch.html">Advanced Search</a> for
 * an overview and a list of 
 * <a href=https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/index.html">available queries</a>
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class AdvancedSearchDataset {    
    /**
     * Runs an RCSB PDB Advanced Search web service using an XML query description.
     * The returned dataset contains the following fields dependent on the query type:
     * <pre> 
     *   structureId, e.g., 1STP
     *   structureChainId, e.g., 4HHB.A
     *   ligandId, e.g., HEM
     * </pre>
     *   
     * @param xmlQuery RCSB PDB Advanced Query XML
     * @return dataset of ids
     * @throws IOException
     */
    public static Dataset<Row> getDataset(String xmlQuery) throws IOException {
        // run advanced query
        List<String> results = AdvancedQueryService.postQuery(xmlQuery);

        // convert list of lists to a dataframe
        SparkSession spark = SparkSession.builder().getOrCreate();

        // handle 3 types of results based on length of string:
        //   structureId: 4 (e.g., 4HHB)
        //   structureEntityId: > 4 (e.g., 4HHB:1)
        //   entityId: < 4 (e.g., HEM)
        Dataset<Row> ds = null;
        if (results.size() > 0) {
            if (results.get(0).length() > 4) {
                ds = spark.createDataset(results, Encoders.STRING()).toDF("structureEntityId");
            
                // if results contain an entity id, e.g., 101M:1, then map entityId to structureChainId
                ds = ds.withColumn("structureId", substring_index(col("structureEntityId"), ":", 1));
                ds = ds.withColumn("entityId", substring_index(col("structureEntityId"), ":", -1));
              
                Dataset<Row> mapping = getEntityToChainId();
                ds = ds.join(mapping, ds.col("structureId").equalTo(mapping.col("structureId")).and(ds.col("entityId").equalTo(mapping.col("entity_id"))));
            
                ds = ds.select("structureChainId");
            } else if (results.get(0).length() < 4) {
                ds = spark.createDataset(results, Encoders.STRING()).toDF("ligandId");
            } else {
                ds = spark.createDataset(results, Encoders.STRING()).toDF("structureId");
            }
        }

        return ds;
    }

    private static Dataset<Row> getEntityToChainId() throws IOException {
        // get entityID to strandId mapping
        String query = "SELECT pdbid, entity_id, pdbx_strand_id FROM entity_poly";
        Dataset<Row> mapping = PdbjMineDataset.getDataset(query);

        // split one-to-many relationship into multiple records: 'A,B -> [A, B] -> explode to separate rows
        mapping = mapping.withColumn("chainId", split(mapping.col("pdbx_strand_id"), ","));
        mapping = mapping.withColumn("chainId", explode(col("chainId")));

        // create a structureChainId file, e.g. 1XYZ + A -> 1XYZ.A
        mapping = mapping.withColumn("structureChainId", concat_ws(".", col("structureId"), col("chainId")));

        return mapping.select("entity_id", "structureId", "structureChainId");
    }
}
