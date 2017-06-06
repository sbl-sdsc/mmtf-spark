package edu.sdsc.mmtf.spark.webservices;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

public class AdvancedQueryService {
	public static final String SERVICELOCATION="http://www.rcsb.org/pdb/rest/search";

	/** 
	 * Post an XML query (PDB XML query format)  to the RESTful RCSB web service
	 * 
	 * @param xml
	 * @return a list of PDB ids.
	 */
	public List<String> postQuery(String xml) throws IOException{
		URL u = new URL(SERVICELOCATION);

		String encodedXML = URLEncoder.encode(xml,"UTF-8");
		InputStream in =  doPOST(u,encodedXML);

		List<String> pdbIds = new ArrayList<String>();

		BufferedReader rd = new BufferedReader(new InputStreamReader(in));

		String line;
		while ((line = rd.readLine()) != null) {
			pdbIds.add(line);
		}      
		rd.close();

		return pdbIds;
	}

	/** 
	 * POST to a URL and return the response stream.
	 * 
	 * @param url
	 * @return
	 * @throws IOException
	 */
	public static InputStream doPOST(URL url, String data) throws IOException {
		URLConnection conn = url.openConnection();
		conn.setDoOutput(true);

		OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());

		wr.write(data);
		wr.flush();

		// Get the response
		return conn.getInputStream();
	}
}
