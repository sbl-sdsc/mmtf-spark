package edu.sdsc.mmtf.spark.utils;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * 
 * @author Yue Yu
 *
 */

public class PiscesDownload {
	
	private int sequenceIdentity = 0;
	private double resolution = 0.0;
	private double rFactor = 0.0;
	private static final String URL = "http://dunbrack.fccc.edu/Guoli/culledpdb_hh";
	private static final List<Integer> seqIDList = Arrays.asList(20,25,30,40,50,60,70,80,90);
	private static final List<Double> resolutionList = Arrays.asList(1.6,1.8,2.0,2.2,2.5,3.0);
	private static final List<Double> rFactorList = Arrays.asList(0.25,1.0);
	
	
	public PiscesDownload(int sequenceIdentity, double resolution, double rFactor)
	{
		//check input for validity
		if( !seqIDList.contains(sequenceIdentity) ) {
			throw new IllegalArgumentException("Invalid sequenceIdentity");
	    }
		if( !resolutionList.contains(resolution) ) {
			throw new IllegalArgumentException("Invalid resolution value");
	    }
		if( !rFactorList.contains(rFactor) ) {
			throw new IllegalArgumentException("Invalid rFactor value");
	    }
		this.sequenceIdentity = sequenceIdentity;
		this.resolution = resolution;
		this.rFactor = rFactor;
	}
	
	public List<String> getPdbIds() throws IOException
	{
		String fileURL = getFileName();
		if( fileURL.equals("") ) {
			throw new IllegalArgumentException("File Not Found");
	    }
		URL u = new URL(fileURL);
		URLConnection conn = u.openConnection();
		List<String> pdbIDs = new ArrayList<String>();
		
		InputStream in = conn.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
		String line;
		while((line = rd.readLine()) != null)
		{
		//	System.out.println(line);
			pdbIDs.add(line.substring(0,4).concat(".").concat(line.substring(4,5)));
		}
		return pdbIDs;
	}
	private String getFileName() throws IOException
	{
		URL u = new URL(URL);
		URLConnection conn = u.openConnection();
		InputStream in = conn.getInputStream();
		String fileName = "";
		String cs = "pc" + Integer.toString(sequenceIdentity) + "_res" + Double.toString(resolution) + "_R" + Double.toString(rFactor);
		//System.out.println("the string is : " + cs);
		BufferedReader rd = new BufferedReader(new InputStreamReader(in));
		String line;
		while((line = rd.readLine()) != null)
		{
			line = line.substring(line.indexOf("\"") + 1);
			line = line.substring(0, line.lastIndexOf("\""));
			if(line.contains(cs) && !line.contains("fasta") )
			{
				fileName = line;
				//System.out.println(fileName);
			}
		}
		rd.close();
		return fileName;
	}
}
