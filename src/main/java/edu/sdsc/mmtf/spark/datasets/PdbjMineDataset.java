package edu.sdsc.mmtf.spark.datasets;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This filter runs an PDBj Mine 2 Search web service using an SQL query.
 * 
 * <p>
 * See <a href="https://pdbj.org/help/mine2-sql"> Mine 2 SQL</a>
 * <p>
 * Design queries using the <a href="https://pdbj.org/mine/sql">PDBj Mine 2
 * query service</a>.
 * 
 * @author Gert-Jan Bekker
 * @since 0.1.0
 *
 */
public class PdbjMineDataset {
    private static final String SERVICELOCATION = "https://pdbj.org/rest/mine2_sql";

    /**
     * Fetches data using the PDBj Mine 2 SQL service
     * 
     * @param sqlQuery
     *            query in SQL format
     * @throws IOException
     */
    public static Dataset<Row> getDataset(String sqlQuery) throws IOException {
        String encodedSQL = URLEncoder.encode(sqlQuery, "UTF-8");

        URL u = new URL(SERVICELOCATION + "?format=csv&q=" + encodedSQL);
        InputStream in = u.openStream();

        // save as a temporary CSV file
        Path tempFile = Files.createTempFile(null, ".csv");
        Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        in.close();
        
        SparkSession spark = SparkSession.builder().getOrCreate();

        // load temporary CSV file into Spark dataset
        return spark.read()
                .format("csv").option("header", "true")
                .option("inferSchema", "true")
 //               .option("parserLib", "UNIVOCITY") 
                .load(tempFile.toString());
    }
}
