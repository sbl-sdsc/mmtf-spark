package edu.sdsc.mmtf.spark.datasets;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * This class downloads the dataset used to train the
 * <a href="http://www.compbio.dundee.ac.uk/jpred/about_RETR_JNetv231_details.shtml">JPred 4/JNet (v.2.3.1)</a>
 * secondary structure predictor. It can be used as a reference
 * dataset for machine learning applications.
 * This dataset includes the ScopID, sequence, DSSP secondary
 * structure assignment, and a flag that indicates if data point
 * was part of the training set.
 * 
 * @author Yue Yu
 * 
 */
public class JpredDataset {
	/**
	 * Gets JPred 4/JNet (v.2.3.1) secondary structure dataset.
	 * @param sc JavaSparkContext
	 * @return secondary structure dataset
	 * @throws IOException if file cannot be downloaded or read
	 */
	public static Dataset<Row> getDataset(JavaSparkContext sc) throws IOException {	
		List<Row> res = new ArrayList<Row>();
		String URL = "http://www.compbio.dundee.ac.uk/jpred/downloads/retr231.tar.gz";		
		URL u = new URL(URL);
		URLConnection conn = u.openConnection();
		InputStream in = conn.getInputStream();
		BufferedInputStream fin = new BufferedInputStream(in);
		GzipCompressorInputStream gzIn = new GzipCompressorInputStream(fin);
		TarArchiveInputStream tarIn = new TarArchiveInputStream(gzIn);
		TarArchiveEntry entry = null;
		
		Set<String> scopIDs = new HashSet<>(); 
		Map<String, String> sequences = new HashMap<String, String>();
		Map<String, String> secondaryStructures = new HashMap<String, String>();
		Map<String, String> trained = new HashMap<String, String>();
		
		while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
			if (entry.isDirectory()) {
				continue;
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(tarIn));
			if(entry.getName().contains(".dssp"))
			{
				String scopID =  br.readLine().substring(1);
				String secondaryStructure = br.readLine();
				secondaryStructure = secondaryStructure.replace("-", "C");
				secondaryStructures.put(scopID, secondaryStructure);
			}
			else if(entry.getName().contains(".fasta"))
			{
				String scopID =  br.readLine().substring(1);
				String sequence = br.readLine();
				scopIDs.add(scopID);
				sequences.put(scopID, sequence);
				if(entry.getName().contains("training/"))
					trained.put(scopID, "true");
				else if(entry.getName().contains("blind/"))
					trained.put(scopID, "false");
			}
		}
		tarIn.close();
		
		Iterator<String> iter = scopIDs.iterator();
		while (iter.hasNext()) {
			String scopID = iter.next();
		    res.add(RowFactory.create(scopID, sequences.get(scopID), secondaryStructures.get(scopID),
		    		trained.get(scopID)));
		}
		
		JavaRDD<Row> data = sc.parallelize(res);
		return JavaRDDToDataset.getDataset(data, "scopID", "sequence", "secondaryStructure", "trained");
	}
}