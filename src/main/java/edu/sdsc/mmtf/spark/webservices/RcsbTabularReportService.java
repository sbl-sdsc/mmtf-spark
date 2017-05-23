package edu.sdsc.mmtf.spark.webservices;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class uses RCSB PDB Tabular Report RESTful web services to retrieve metadata
 * and annotations for all current entries in the Protein Data Bank.
 * See <a href="https://www.rcsb.org/pdb/results/reportField.do">for list of supported
 * field names.</a>
 * Reference: The RCSB Protein Data Bank: redesigned web site and web services 2011
 * Nucleic Acids Res. 39: D392-D401.
 * See <a href="https://dx.doi.org/10.1093/nar/gkq1021">doi:10.1093/nar/gkq1021</a>
 * 
 * Example: Retrieve PubMedCentral, PubMed ID, and Deposition date
 * 
 *      RcsbTabularReportService service = new RcsbTabularReportService();
 *
 *      List<String> columnNames = Arrays.asList("pmc","pubmedId","depositionDate");
 *		Dataset<Row> ds = service.getDataset(columnNames);
 *      ds.printSchema();
 *      ds.show(5);
 *
 * @author Peter Rose
 * 
 */
public class RcsbTabularReportService {
	public static final String SERVICELOCATION="http://www.rcsb.org/pdb/rest/customReport";
	private static final String CURRENT_URL = "?pdbids=*&service=wsfile&format=csv&primaryOnly=1&customReportColumns=";

	/**
	 * Returns a dataset with the specified columns for all current PDB entries.
	 * See <a href="https://www.rcsb.org/pdb/results/reportField.do"> for list of supported
     * field names.
     * 
	 * @param columnNames
	 * @return dataset with the specified columns
	 * @throws IOException
	 */
	public Dataset<Row> getDataset(List<String> columnNames) throws IOException {
		// form query URL
		String query = getUrl(columnNames);
		
		// run tabular report query
		InputStream input = postQuery(query);
		
		// save as a temporary CSV file
		Path tempFile = saveTempFile(input);
		
		// load temporary CSV file into Spark dataset
		return readCsv(tempFile.toString());
	}
	
   /** 
	* Posts PDB Ids and fields in a query string to the RESTful RCSB web service.
	*
	* @param url RESTful query URL
	* @return InputStream 
	*/
   	public InputStream postQuery(String url) throws IOException
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
    private Path saveTempFile(InputStream input) throws IOException {	
		Path tempFile = Files.createTempFile(null, ".csv");
		Files.copy(input, tempFile, StandardCopyOption.REPLACE_EXISTING);
		
		input.close();
		
		return tempFile;
	}

   /** 
   	* Does a POST to a URL and returns the response stream for further processing elsewhere.
	*
	* @param url
	* @return
	* @throws IOException
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

		// Get the response
		return conn.getInputStream();
	}
	
	/**
	 * Creates RESTful URL for tabular report service.
	 * 
	 * @param columnNames
	 * @return
	 */
	public String getUrl(List<String> columnNames) {
		StringBuilder sb = new StringBuilder();
		sb.append(CURRENT_URL);
		
		for (int i = 0; i < columnNames.size(); i++) {
			sb.append(columnNames.get(i));
			if (i != columnNames.size()-1) {
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
	private static Dataset<Row> readCsv(String inputFileName) throws IOException {
	    
	    SparkSession spark = SparkSession
	    		.builder()
	    		.master("local[*]")
	    		.getOrCreate();
	    	    
	    Dataset<Row> df = spark.read()
	    		.format("csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(inputFileName);
       
        return df;
	}
}
