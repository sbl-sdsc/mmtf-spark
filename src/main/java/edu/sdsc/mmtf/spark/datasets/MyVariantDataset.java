package edu.sdsc.mmtf.spark.datasets;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
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
 * This class queries and retrieves missense variations using the MyVariant.info 
 * web services for a list of UniProt ids. 
 * <p>
 * See <a href="http://myvariant.info">MyVariant.info</a> for more information.
 * <p>
 * See <a href="http://myvariant.info/docs/">query syntax</a>.
 * <p>
 * Example 1: Get all missense variations for a list of Uniprot Ids
 * <pre>
 * {@code
 * List<String> uniprotIds = Arrays.asList("P15056"); // BRAF
 * Dataset<Row> ds = MyVariantDataset.getVariations(uniprotIds);
 * ds.show()
 * }
 * </pre>
 * <p>
 * Example 2: Return missense variations that match a query
 * <pre>
 * {@code
 * List<String> uniprotIds = Arrays.asList("P15056"); // BRAF
 * String query = "clinvar.rcv.clinical_significance:pathogenic " 
 *                + "OR clinvar.rcv.clinical_significance:likely pathogenic";
 *                
 * Dataset<Row> ds = MyVariantDataset.getVariations(uniprotIds, query);
 * ds.show()
 * }
 * +-------------------+---------+
 * |        variationId|uniprotId|
 * +-------------------+---------+
 * |chr7:g.140454006G>T|   P15056|
 * |chr7:g.140453153A>T|   P15056|
 * |chr7:g.140477853C>A|   P15056|
 * </pre>
 * 
 * <p>
 * Reference:
 * <p>
 * Xin J, Mark A, Afrasiabi C, Tsueng G, Juchler M, Gopal N, Stupp GS, 
 * Putman TE, Ainscough BJ, Griffith OL, Torkamani A, Whetzel PL, 
 * Mungall CJ, Mooney SD, Su AI, Wu C (2016) High-performance web services 
 * for querying gene and variant annotation. Genome Biology 17(1):1-7
 * <a href="https://doi.org/10.1186/s13059-016-0953-9">doi:10.1186/s13059-016-0953-9</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class MyVariantDataset {
    private static final String MYVARIANT_QUERY_URL = "http://myvariant.info/v1/query?q=";
    private static final String MYVARIANT_SCROLL_URL = "http://myvariant.info/v1/query?scroll_id=";

    /**
     * Returns a dataset of missense variations for a list of Uniprot Ids.
     * 
     * @param uniprotIds list of Uniprot Ids
     * @return dataset with variation Ids and Uniprot Ids or null if no data are found
     * @throws IOException
     */
    public static Dataset<Row> getVariations(List<String> uniprotIds) throws IOException {
        return getVariations(uniprotIds, "");
    }
    
    /**
     * Returns a dataset of missense variations for a list of Uniprot Ids and a MyVariant.info query.
     * See <a href="http://myvariant.info/docs/">query syntax</a>.
     * <p> Example:
     * <pre>
     * String query = "clinvar.rcv.clinical_significance:pathogenic " 
     *                + "OR clinvar.rcv.clinical_significance:likely pathogenic";
     * </pre>
     * 
     * @param uniprotIds list of Uniprot Ids
     * @param query MyVariant.info query string
     * @return dataset with variation Ids and Uniprot Ids or null if no data are found
     * @throws IOException
     */
    public static Dataset<Row> getVariations(List<String> uniprotIds, String query) throws IOException {
        // get a spark context
        SparkSession spark = SparkSession.builder().getOrCreate();
        @SuppressWarnings("resource") // sc will be closed elsewhere
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // download data in parallel
        JavaRDD<String> data = sc.parallelize(uniprotIds).flatMap(m -> getData(m, query));

        // convert from JavaRDD to Dataset
        Dataset<String> jsonData = spark.createDataset(JavaRDD.toRDD(data), Encoders.STRING());

        // parse json strings and return as a dataset
        Dataset<Row> dataset = spark.read().json(jsonData);

        // return null if dataset contains no results
        if (!Arrays.asList(dataset.columns()).contains("hits")) {
            System.out.println("MyVariantDataset: no matches found");
            return null;
        }

        return flattenDataset(dataset);
    }

    private static Iterator<String> getData(String uniprotId, String query) throws IOException {
        List<String> data = new ArrayList<>();

        // create url
        String url = MYVARIANT_QUERY_URL + "snpeff.ann.effect:missense_variant AND dbnsfp.uniprot.acc:" + uniprotId;
        if (!query.isEmpty()) {
            url = url + " AND (" + query + ")";
        }
        url = url + "&fields=_id&fetch_all=true";
        url = url.replaceAll(" ", "%20");
        URL u = new URL(url);

        // open input stream
        InputStream is = null;
        try {
            is = u.openStream();
        } catch (IOException e) {
            System.err.println("WARNING: Could not load data for: " + uniprotId);
            return data.iterator();
        }

        // read results
        // A maximum of 1000 records are returned per request.
        // Additional records are retrieved iteratively using the scrollId.
        String results = readResults(is);
        is.close();

        if (gotHits(results)) {
            results = addUniprotId(results, uniprotId);
            data.add(results);

            String scrollId = getScrollId(results);

            while (scrollId != null) {
                u = new URL(MYVARIANT_SCROLL_URL + scrollId);
                try {
                    is = u.openStream();
                } catch (IOException e) {
                    System.err.println("WARNING: Could not load data for: " + uniprotId);
                    continue;
                }

                results = readResults(is);
                is.close();

                if (gotHits(results)) {
                    results = addUniprotId(results, uniprotId);
                    data.add(results);
                    scrollId = getScrollId(results);
                } else {
                    scrollId = null;
                }
            }
        }

        return data.iterator();
    }

    /**
     * Converts data read from input stream into a single line of text.
     * 
     * @param is
     *            input stream
     * @return response as a string
     * @throws IOException
     */
    private static String readResults(InputStream is) throws IOException {
        List<String> results = IOUtils.readLines(is, "UTF-8");
        StringBuilder sb = new StringBuilder();
        for (String line : results) {
            sb.append(line);
        }
        return sb.toString();
    }

    /**
     * Adds uniprotId to each json record.
     * 
     * <pre>
     * Example:
     * replace
     *    "hits": ...
     * with
     *    "uniprotId":"P15056","hits": ...
     * </pre>
     * 
     * @param line
     *            line of json
     * @param uniprotId
     */
    private static String addUniprotId(String line, String uniprotId) {
        String ids = "\"uniprotId\":" + "\"" + uniprotId + "\"," + "\"hits\"";
        return line.replaceAll("\"hits\"", ids);
    }

    private static String getScrollId(String line) throws IOException {
        if (line.contains("_scroll_id")) {
            return line.split("\"")[3];
        }
        return null;
    }

    /**
     * Returns true if line contains query hits
     * @param line
     * @return
     * @throws IOException
     */
    private static boolean gotHits(String line) throws IOException {
        if (line.contains("hits"))
            return true;
        if (line.contains("No results to return"))
            return false;

        return false;
    }

    /**
     * Converts dataset into a simple flat data scheme.
     * @param ds
     * @return
     */
    private static Dataset<Row> flattenDataset(Dataset<Row> ds) {
        return ds.withColumn("variationId", explode(ds.col("hits._id"))).select(col("variationId"), col("uniprotId"));
    }
}