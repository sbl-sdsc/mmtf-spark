package edu.sdsc.mmtf.spark.datasets;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.zip.ZipInputStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * This class provides access to DrugBank datasets containing drug structure and
 * drug target information. Theses datasets contain identifiers and names for
 * integration with other data resources. See
 * <a href="https://www.drugbank.ca/">Drug Bank Downloads</a>.
 * 
 * <p>
 * Reference: Wishart DS, et al., DrugBank 5.0: a major update to the DrugBank
 * database for 2018. Nucleic Acids Res. 2017 Nov 8. See
 * <a href="https://dx.doi.org/10.1093/nar/gkx1037">doi:10.1093/nar/gkx1037</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class DrugBankDataset {
    public enum DrugGroup {
        ALL, APPROVED, EXPERIMENTAL, NUTRACEUTICAL, ILLICIT, WITHDRAWN, INVESTIGATIONAL
    };

    public enum DrugType {
        SMALL_MOLECULE, BIOTECH
    };

    private static final String BASE_URL = "https://www.drugbank.ca/releases/latest/downloads/";
    private static final int BUFFER = 2048;

    /**
     * Downloads the DrugBank Open Data dataset with drug structure external
     * links and identifiers. See DrugBank
     * <a href="https://www.drugbank.ca/releases/latest#open-data">Open Data
     * dataset</a>.
     *
     * <p>
     * This dataset contains drug common names, synonyms, CAS numbers, and
     * Standard InChIKeys.
     * 
     * <p>
     * The DrugBank Open Data dataset is a public domain dataset that can be
     * used freely in your application or project (including commercial use). It
     * is released under a Creative Common’s CC0 International License.
     * 
     * <p>Example: Get DrugBank open dataset
     * <pre><code>
     * Dataset<Row> openDrugLinks = DrugBankDataset.getOpenDrugLinks();
     * openDrugLinks.show();
     * </pre></code>
     * 
     * @return DrugBank dataset
     * @throws IOException
     */
    public static Dataset<Row> getOpenDrugLinks() throws IOException {
        String url = BASE_URL + "all-drugbank-vocabulary";
        return getDataset(url);
    }

    /**
     * Downloads drug structure external links and identifiers from DrugBank.
     * Either all or subsets of data can be downloaded by specifying the
     * DrugGroup: ALL, APPROVED, EXPERIMENTAL, NUTRACEUTICAL, ILLLICT,
     * WITHDRAWN, INVESTIGATIONAL. See DrugBank
     * <a href="https://www.drugbank.ca/releases/latest#external-links">External
     * Drug Links</a>.
     *
     * <p>
     * The structure external links datasets include drug structure information
     * in the form of InChI/InChI Key/SMILES as well as identifiers for other
     * drug-structure resources (such as ChEBI, ChEMBL,ChemSpider, BindingDB,
     * etc.). Included in each dataset is also the PubChem Compound ID (CID) and
     * the particular PubChem Substance ID (SID) for the given DrugBank record.
     * 
     * <p>
     * These DrugBank datasets are released under a Creative Common’s
     * Attribution-NonCommercial 4.0 International License. They can be used
     * freely in your non-commercial application or project. A DrugBank user
     * account and authentication is required to download these datasets.
     * 
     * <p>
     * Example: Get dataset of external links and identifiers of approved drugs
     * 
     * <pre><code>
     * String username = args[0];
     * String password = args[1];
     * Dataset<Row> drugLinks = getDrugLinks(DrugGroup.APPROVED, username, password);
     * drugLinks.show();
     * </pre></code>
     * 
     * @param drugGroup
     *            specific dataset to be downloaded
     * @param username
     *            DrugBank username
     * @param password
     *            DrugBank password
     * @return DrugBank dataset
     * @throws IOException
     */
    public static Dataset<Row> getDrugLinks(DrugGroup drugGroup, String username, String password) throws IOException {
        String url = BASE_URL + drugGroup + "-structure-links";
        return getDataset(url, username, password);
    }

    /**
     * Downloads drug target external links and identifiers from DrugBank.
     * Either all or subsets of data can be downloaded by specifying the
     * DrugGroup: ALL, APPROVED, EXPERIMENTAL, NUTRACEUTICAL, ILLLICT,
     * WITHDRAWN, INVESTIGATIONAL. See DrugBank
     * <a href="https://www.drugbank.ca/releases/latest#external-links">Target
     * Drug-UniProt Links</a>.
     *
     * <p>
     * The drug target external links datasets include drug name, drug type
     * (small molecule, biotech), UniProtID and UniProtName.
     * 
     * <p>
     * These DrugBank datasets are released under the Creative Common’s
     * Attribution-NonCommercial 4.0 International License. They can be used
     * freely in your non-commercial application or project. A DrugBank user
     * account and authentication is required to download these datasets.
     * 
     * <p>
     * Example: Get dataset of drug target external links and identifiers of all
     * drugs in DrugBank
     * 
     * <pre><code>
     * String username = args[0];
     * String password = args[1];
     * Dataset<Row> drugTargetLinks = getDrugTargetLinks(DrugGroup.ALL, username, password);
     * drugTargetLinks.show();
     * </pre></code>
     * 
     * @param drugGroup
     *            specific dataset to be downloaded
     * @param username
     *            DrugBank username
     * @param password
     *            DrugBank password
     * @return DrugBank dataset
     * @throws IOException
     */
    public static Dataset<Row> getDrugTargetLinks(DrugGroup drugGroup, String username, String password)
            throws IOException {
        String url = BASE_URL + "target-" + drugGroup + "-uniprot-links";
        return getDataset(url, username, password);
    }

    /**
     * Downloads drug target external links and identifiers from DrugBank.
     * Either all or subsets of data can be downloaded by specifying the
     * DrugType: SMALL_MOLECULE, BIOTECH. See DrugBank
     * <a href="https://www.drugbank.ca/releases/latest#external-links">Target
     * Drug-UniProt Links</a>.
     *
     * <p>
     * The drug target external links datasets include drug name, drug
     * type(small molecule, biotech), UniProtID and UniProtName.
     * 
     * <p>
     * These DrugBank datasets are released under the Creative Common’s
     * Attribution-NonCommercial 4.0 International License. They can be used
     * freely in your non-commercial application or project. A DrugBank user
     * account and authentication is required to download these datasets.
     * 
     * <p>
     * Example: Get dataset of drug target external links and identifiers of
     * small-molecule drugs in DrugBank
     * 
     * <pre><code>
     * String username = args[0];
     * String password = args[1];
     * Dataset<Row> drugTargetLinks = getDrugTargetLinks(DrugType.SMALL_MOLECULE, username, password);
     * drugTargetLinks.show();
     * </pre><code>
     * 
     * @param drugType
     *            specific dataset to be downloaded
     * @param username
     *            DrugBank username
     * @param password
     *            DrugBank password
     * @return DrugBank dataset
     * @throws IOException
     */
    public static Dataset<Row> getDrugTargetLinks(DrugType drugType, String username, String password)
            throws IOException {
        String url = BASE_URL + "target-" + drugType + "-uniprot-links";
        return getDataset(url, username, password);
    }

    /**
     * Downloads a DrugBank dataset that does not require authentication.
     * 
     * @param url
     *            DrugBank dataset download link
     * @return DrugBank dataset
     * @throws IOException
     */
    public static Dataset<Row> getDataset(String url) throws IOException {
        // get input stream to first zip entry
        URL u = new URL(url);
        InputStream unzipped = decodeAsZipInputStream(u.openStream());

        // save data to a temporary file (Dataset csv reader requires an input
        // file!)
        Path tempFile = saveTempFile(unzipped);
        unzipped.close();

        // load temporary CSV file into Spark dataset
        Dataset<Row> dataset = readCsv(tempFile.toString());
        dataset = removeSpacesFromColumnNames(dataset);

        return dataset;
    }

    /**
     * Downloads a DrugBank dataset that requires authentication.
     * 
     * @param url
     *            DrugBank dataset download link
     * @param username
     *            DrugBank username
     * @param password
     *            DrugBank password
     * @return DrugBank dataset
     * @throws IOException
     */
    public static Dataset<Row> getDataset(String url, String username, String password) throws IOException {
        // get input stream to zipped file
        InputStream zipped = getInputStream(url, username, password);

        // get input stream to first zip entry
        InputStream unzipped = decodeAsZipInputStream(zipped);

        // save data to a temporary file (Dataset csv reader requires an input
        // file!)
        Path tempFile = saveTempFile(unzipped);
        zipped.close();
        unzipped.close();

        // load temporary CSV file into Spark dataset
        Dataset<Row> dataset = readCsv(tempFile.toString());

        // remove spaces from column names. These cause problems, e.g., when
        // saving files.
        dataset = removeSpacesFromColumnNames(dataset);

        return dataset;
    }

    /**
     * Returns an input stream for a given URL using authentication. This method
     * follows redirected URLs.
     * 
     * @param url
     * @param username
     * @param password
     * @return
     * @throws IOException
     */
    private static InputStream getInputStream(String url, String username, String password) throws IOException {
        // setup authentication
        String authentication = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
        String authHeader = "Basic " + authentication;

        // setup http client with authorization
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(url).get().addHeader("authorization", authHeader).build();

        // return response as an input stream
        Response response = client.newCall(request).execute();
        return response.body().byteStream();
    }

    /**
     * Returns an input stream to the first zip file entry;
     * 
     * @return unzipped InputStream
     * @throws IOException
     */
    private static InputStream decodeAsZipInputStream(InputStream in) throws IOException {
        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(in));

        // get first entry
        zis.getNextEntry();

        // copy data to a byte array output stream
        int count;
        byte data[] = new byte[BUFFER];
        ByteArrayOutputStream dest = new ByteArrayOutputStream();
        while ((count = zis.read(data, 0, BUFFER)) != -1) {
            dest.write(data, 0, count);
        }
        zis.close();

        // convert to an input stream
        return new ByteArrayInputStream(dest.toByteArray());
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
        input.close();

        // TODO delete tempFile
        return tempFile;
    }

    /**
     * Reads CSV file into a Spark dataset
     * 
     * @param fileName
     * @throws IOException
     */
    private static Dataset<Row> readCsv(String inputFileName) throws IOException {
        SparkSession spark = SparkSession.builder().getOrCreate();

        Dataset<Row> dataset = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
                .load(inputFileName);

        return dataset;
    }

    /**
     * Removes spaces from column names to ensure compatibility with parquet
     * files.
     *
     * @param original
     *            dataset
     * @return dataset with columns renamed
     */
    private static Dataset<Row> removeSpacesFromColumnNames(Dataset<Row> original) {

        for (String existingName : original.columns()) {
            String newName = existingName.replaceAll(" ", "");
            original = original.withColumnRenamed(existingName, newName);
        }

        return original;
    }
}