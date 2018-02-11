package edu.sdsc.mmtf.spark.datasets;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class provides access to SWISS-MODEL datasets containing homology
 * models. See <a href=
 * "https://swissmodel.expasy.org/docs/repository_help#smr_api">SWISS-MODEL
 * API</a>.
 * 
 * <p>
 * References:
 * <p>
 * Bienert S, Waterhouse A, de Beer TA, Tauriello G, Studer G, Bordoli L,
 * Schwede T (2017). The SWISS-MODEL Repository - new features and
 * functionality, Nucleic Acids Res. 45(D1):D313-D319.
 * <a href="https://dx.doi.org/10.1093/nar/gkw1132">doi:10.1093/nar/gkw1132</a>.
 * 
 * <p>
 * Biasini M, Bienert S, Waterhouse A, Arnold K, Studer G, Schmidt T, Kiefer F,
 * Gallo Cassarino T, Bertoni M, Bordoli L, Schwede T(2014). The SWISS-MODEL
 * Repository - modelling protein tertiary and quaternary structure using
 * evolutionary information, Nucleic Acids Res. 42(W1):W252–W258.
 * <a href="https://doi.org/10.1093/nar/gku340">doi:10.1093/nar/gku340</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class SwissModelDataset {
    private static final String SWISS_MODEL_REST_URL = "https://swissmodel.expasy.org/repository/uniprot/";
    private static final String SWISS_MODEL_PROVIDER = ".json?provider=swissmodel";
    private static final String PDB_PROVIDER = ".json?provider=pdb";

    /**
     * Downloads metadata for SWISS-MODEL homology models for a list of UniProtIds. 
     * The original data schema is flattened into a row-based schema.
     * 
     * <p> Example:
     * <pre>
     * <code>
     *  List<String> uniProtIds = Arrays.asList("P36575","P24539","O00244");
     *  Dataset<Row> ds = SwissProtDataset.getSwissModels(uniProtIds);
     *  ds.show();
     * </code>
     * +------+--------+----+---+-----+----------+----+--------+-----------+--------+--------+--------+----------+-----------+
     * |    ac|sequence|from| to|qmean|qmean_norm|gmqe|coverage|oligo-state|  method|template|identity|similarity|coordinates|
     * +------+--------+----+---+-----+----------+----+--------+-----------+--------+--------+-- -----+----------+-----------+
     * |P36575|MSKVF...|   2|371|-3.06|0.66345522|0.75|0.953608|    monomer|Homology|1suj.1.A|68.66484|0.50463312|https://...|
     * |P24539|MLSRV...|  76|249|-2.51|0.67113881|0.65|0.679687|    monomer|Homology|5ara.1.S|84.48275|0.54788881|https://...|
     * |O00244|MPKHE...|   1| 68| 1.04|0.84233218|0.98|     1.0| homo-2-mer|Homology|1fe4.1.A|   100.0|0.60686457|https://...|
     * +------+--------+----+---+-----+----------+----+--------+-----------+--------+--------+--------+----------+-----------+
     * </pre>
     * 
     * @param uniProtIds
     *            list of UniProt Ids
     * @return SwissModel dataset
     * @throws IOException
     */
    public static Dataset<Row> getSwissModels(List<String> uniProtIds) throws IOException {
        Dataset<Row> dataset = getSwissModelsRawData(uniProtIds);
        return flattenDataset(dataset);
    }

    /**
     * Downloads the raw metadata for SWISS-MODEL homology models. This dataset
     * is in the original data schema as downloaded from SWISS-MODEL.
     * 
     * @param uniProtIds
     *            list of UniProt Ids
     * @return SwissModel dataset in original data schema
     * @throws IOException
     */
    public static Dataset<Row> getSwissModelsRawData(List<String> uniProtIds) throws IOException {
        List<Path> paths = new ArrayList<>(uniProtIds.size());

        for (String uniProtId : uniProtIds) {
            InputStream is = null;
            URL u = new URL(SWISS_MODEL_REST_URL + uniProtId + SWISS_MODEL_PROVIDER);
            try {
                is = u.openStream();
                if (is == null) {
                    System.err.println("WARNING: Could not load data for: " + uniProtId);
                    continue;
                }
            } catch (IOException e) {
                System.err.println("WARNING: Could not load data for: " + uniProtId);
                continue;
            }

            // save data to a temporary file requires as input to dataset reader
            paths.add(saveTempFile(is));
            is.close();
        }

        // load temporary JSON file into Spark dataset
        Dataset<Row> dataset = readJsonFiles(paths);

        // TODO this doesn't work since files are still in use
        // deleteTempFiles(paths); 

        return dataset;
    }

    /**
     * Flattens the original hierarchical data schema into a simple row-based
     * schema. Some less useful data are excluded.
     * 
     * @param original
     *            hierarchical dataset
     * @return flattened dataset
     */
    private static Dataset<Row> flattenDataset(Dataset<Row> ds) {
        return ds.withColumn("structures", explode(ds.col("result.structures"))).select(col("query.ac"),
                col("result.sequence"), col("structures.from"), col("structures.to"), 
                col("structures.qmean"), col("structures.qmean_norm"), col("structures.gmqe"),
                col("structures.coverage"), col("structures.oligo-state"), col("structures.method"),
                col("structures.template"), col("structures.identity"), col("structures.similarity"),
                col("structures.coordinates"), col("result.md5"), col("structures.md5"));
    }

    /**
     * Saves tabular report as a temporary CSV file.
     * 
     * @param input
     * @return path to temporary file
     * @throws IOException
     */
    private static Path saveTempFile(InputStream input) throws IOException {
        Path tempFile = Files.createTempFile(null, ".csv");
        Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
        return tempFile;
    }

    /**
     * Reads JSON files into a Spark dataset
     * 
     * @param paths
     *            JSON files
     * @throws IOException
     */
    private static Dataset<Row> readJsonFiles(List<Path> paths) throws IOException {
        // create an array of file names
        String[] fileNames = new String[paths.size()];
        int n = 0;
        for (Path path : paths) {
            fileNames[n++] = path.toString();
        }

        // load files into dataset
        SparkSession spark = SparkSession.builder().getOrCreate();
        return spark.read().format("json").load(fileNames);
    }

//    private static void deleteTempFiles(List<Path> paths) throws IOException {
//        for (Path path : paths) {
//            Files.deleteIfExists(path);
//        }
//    }
}