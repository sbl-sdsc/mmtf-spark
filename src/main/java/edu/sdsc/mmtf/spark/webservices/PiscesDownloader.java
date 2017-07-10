package edu.sdsc.mmtf.spark.webservices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.net.URLConnection;

/**
 * This class downloads representative protein chains from the PISCES 
 * CulledPDB sets. A CulledPDB set is selected by specifying
 * sequenceIdentity, resolution, and rValue cutoff values from the following
 * list:
 * <p> sequenceIdentity = 20, 25, 30, 40, 50, 60, 70, 80, 90
 * <p> resolution = 1.6, 1.8, 2.0, 2.2, 2.5, 3.0
 * 
 * <p> See <a href="http://dunbrack.fccc.edu/PISCES.php">PISCES</a>.
 * Please cite the following in any work that uses lists provided by PISCES
 * G. Wang and R. L. Dunbrack, Jr. PISCES: a protein sequence culling server. 
 * Bioinformatics, 19:1589-1591, 2003.
 *  
 * @author Yue Yu
 *
 */
public class PiscesDownloader implements Serializable {	
	private static final long serialVersionUID = -8421057562643603786L;
	
	private static final String URL = "http://dunbrack.fccc.edu/Guoli/culledpdb_hh";
	private static final List<Integer> SEQ_ID_LIST = Arrays.asList(20,25,30,40,50,60,70,80,90);
	private static final List<Double> RESOLUTION_LIST = Arrays.asList(1.6,1.8,2.0,2.2,2.5,3.0);

	private int sequenceIdentity = 0;
	private double resolution = 0.0;
	
	/**
	 * <p> sequenceIdentity = 20, 25, 30, 40, 50, 60, 70, 80, 90
	 * <p> resolution = 1.6, 1.8, 2.0, 2.2, 2.5, 3.0
	 * @param sequenceIdentity sequence identity cutoff value
	 * @param resolution resolution cutoff value
	 * @param rValue rValue cutoff value
	 */
	public PiscesDownloader(int sequenceIdentity, double resolution)
	{
		//check input for validity
		if( !SEQ_ID_LIST.contains(sequenceIdentity) ) {
			throw new IllegalArgumentException("Invalid sequenceIdentity");
	    }
		if( !RESOLUTION_LIST.contains(resolution) ) {
			throw new IllegalArgumentException("Invalid resolution value");
	    }

		this.sequenceIdentity = sequenceIdentity;
		this.resolution = resolution;
	}
	
	/**
	 * Return a list of PdbId.ChainNames (e.g., 4R4X.A) for the arguments 
	 * specified in the constructor.
	 * 
	 * @return list of PdbId.ChainNames
	 * @throws IOException if data set cannot be downloaded from PISCES server.
	 */
	public List<String> getStructureChainIds() throws IOException
	{
		String fileURL = getFileName();
		URL u = new URL(fileURL);
		URLConnection conn = u.openConnection();
		List<String> structureChainId = new ArrayList<String>();
		
		InputStream in = conn.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
		
		String line;
		while((line = rd.readLine()) != null)
		{
			structureChainId.add(line.substring(0,4).concat(".").concat(line.substring(4,5)));
		}
		return structureChainId;
	}
	
	private String getFileName() throws IOException
	{
		URL u = new URL(URL);
		URLConnection conn = u.openConnection();
		InputStream in = conn.getInputStream();
		String fileName = "";
		String cs = "pc" + Integer.toString(sequenceIdentity) + "_res" + Double.toString(resolution);

		BufferedReader rd = new BufferedReader(new InputStreamReader(in));
		String line;
		while((line = rd.readLine()) != null)
		{
			line = line.substring(line.indexOf("\"") + 1);
			line = line.substring(0, line.lastIndexOf("\""));
			if(line.contains(cs) && !line.contains("fasta") )
			{
				fileName = line;
			}
		}
		rd.close();
		return fileName;
	}
}
