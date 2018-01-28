package edu.sdsc.mmtf.spark.datasets;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class uses RCSB PDB Tabular Report RESTful web services to retrieve metadata
 * and annotations for all current entries in the Protein Data Bank.
 * See <a href="http://www.rcsb.org/pdb/results/reportField.do">for list of supported
 * field names.</a>
 * <p>Reference: The RCSB Protein Data Bank: redesigned web site and web services 2011
 * Nucleic Acids Res. 39: D392-D401.
 * See <a href="https://dx.doi.org/10.1093/nar/gkq1021">doi:10.1093/nar/gkq1021</a>
 * 
 * <p>Example: Retrieve PubMedCentral, PubMed ID, and Deposition date
 * <pre>
 * {@code
 * Dataset<Row> ds = CustomReportService.getDataset("pmc","pubmedId","depositionDate");
 * ds.printSchema();
 * ds.show(5);
 * }
 * </pre>
 * @author Peter Rose
 * @since 0.1.0
 * 
 */
public class CustomReportService {
	public static final String SERVICELOCATION="http://www.rcsb.org/pdb/rest/customReport";
	private static final String CURRENT_URL = "?pdbids=*&service=wsfile&format=csv&primaryOnly=1&customReportColumns=";

	/**
	 * Returns a dataset with the specified columns for all current PDB entries.
	 * See <a href="https://www.rcsb.org/pdb/results/reportField.do"> for list of supported
     * field names</a>
     * 
	 * @param columnNames names of the columns for the dataset
	 * @return dataset with the specified columns
	 * @throws IOException when temporary csv file cannot be created
	 */
	public static Dataset<Row> getDataset(String... columnNames) throws IOException {	
		// form query URL
		String query = CURRENT_URL + columNamesString(columnNames);
		
		// run tabular report query
		InputStream input = postQuery(query);
		
		// save as a temporary CSV file
		Path tempFile = saveTempFile(input);
		
		SparkSession spark = SparkSession
	    		.builder()
	    		.getOrCreate();
		
		// load temporary CSV file into Spark dataset
		Dataset<Row> dataset = readCsv(spark, tempFile.toString());
		
		return concatIds(spark, dataset, columnNames);
	}
	
	/**
	 * Concatenates structureId and chainId fields into a single key if chainId field
	 * is present
	 * 
	 * @param spark
	 * @param dataset
	 * @return
	 */
	private static Dataset<Row> concatIds(SparkSession spark, Dataset<Row> dataset, String[] columnNames) {
		if (Arrays.asList(dataset.columns()).contains("chainId")) {
			dataset.createOrReplaceTempView("table");
			
			String sql = "SELECT CONCAT(structureId,'.',chainId) as structureChainId," + 
			             "structureId,chainId," + columNamesString(columnNames) + 
			             " from table";
			dataset = spark.sql(sql);
		}
		return dataset;
	}

   /** 
	* Posts PDB Ids and fields in a query string to the RESTful RCSB web service.
	*
	* @param url RESTful query URL
	* @return input stream to response
	*/
   	private static InputStream postQuery(String url) throws IOException
	{
		URL u = new URL(SERVICELOCATION);

		String encodedUrl = URLEncoder.encode(url,"UTF-8");

		InputStream input =  doPOST(u,encodedUrl);
		
		return input;
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
   	* Does a POST to a URL and returns the response stream for further processing elsewhere.
	*
	* @param url RESTFul url
	* @param data the data to be posted
	* @return input stream to response
	* @throws IOException if it cannot open connection
	*/
	public static InputStream doPOST(URL url, String data)
		throws IOException
   	{
		URLConnection conn = url.openConnection();
		conn.setConnectTimeout(60000);
		
		conn.setDoOutput(true);
		
		OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());

		wr.write(data);
		wr.flush();
		wr.close();

		// Get the response
		return conn.getInputStream();
	}
	
	private static String columNamesString(String[] columnNames) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < columnNames.length; i++) {
			sb.append(columnNames[i]);
			if (i != columnNames.length-1) {
			   sb.append(",");
			}
		}
		return sb.toString();
	}
	
	/**
	 * Reads CSV file into a Spark dataset
	 * @param fileName
	 * @throws IOException
	 */
	private static Dataset<Row> readCsv(SparkSession spark, String inputFileName) throws IOException {    	    
	    Dataset<Row> dataset = spark.read()
	    		.format("csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(inputFileName);
       
        return dataset;
	}
}
