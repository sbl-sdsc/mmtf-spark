package edu.sdsc.mmtf.spark.mappers.demos;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.mappers.StructureToCathDomains;

/**
 * Example demonstrating how to extract all possible protein dimers
 * from a biological assembly.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class MapToCathDomains {

	
	private static HashMap<String, ArrayList<String>> getMap(String url) throws IOException
	{
		
		HashMap<String, ArrayList<String>> hmap = new HashMap<String, ArrayList<String>>();
	    
		URL u = new URL(url);
	    InputStream in = u.openStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));

		String key, value, line;

		while ((line = rd.readLine()) != null) {

//			System.out.println(line);
			key = line.substring(0,5).toUpperCase();
			value = line.substring(8).split(" ")[2];
//			System.out.println(key+"\n"+value);
			if(!hmap.containsKey(key))
			{
				ArrayList<String> tmp = new ArrayList<String>();
				tmp.add(value);
				hmap.put(key, tmp);
			}
			else{
				ArrayList<String> tmp = hmap.get(key);
				tmp.add(value);
				hmap.replace(key, tmp);
				
			}
	    
		}
		return hmap;
	}
	
	public static void main(String[] args) throws IOException {

	    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(MapToCathDomains.class.getSimpleName());
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    List<String> pdbIds = Arrays.asList("1HV4"); // single protein chain 5IBZ -> D2
	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc);
	    String baseUrl = "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/daily-release/newest/cath-b-newest-all.gz";
	    HashMap<String, ArrayList<String>> hmap = getMap(baseUrl);
	   
	    pdb = pdb.flatMapToPair(new StructureToCathDomains(hmap));
	   
	    System.out.println("# cathDomains in 1HV4: " + pdb.count());
	    
	    sc.close();
	}
}
