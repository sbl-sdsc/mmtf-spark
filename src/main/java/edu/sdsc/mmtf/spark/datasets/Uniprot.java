package edu.sdsc.mmtf.spark.datasets;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * @author Yue Yu
 * 
 */
public class Uniprot {
	public enum UniDataset{
		SWISS_PROT("ftp://ftp.uniprot.org/pub/databases/"
				+ "uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz"),
		TREMBL("ftp://ftp.uniprot.org/pub/databases/"
				+ "uniprot/current_release/knowledgebase/complete/uniprot_trembl.fasta.gz"),
		UNIREF50("ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref50/uniref50.fasta.gz"),
		UNIREF90("ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref90/uniref90.fasta.gz"),
		UNIREF100("ftp://ftp.uniprot.org/pub/databases/uniprot/uniref/uniref100/uniref100.fasta.gz");
		private final String url;
		UniDataset(String url)
		{
			this.url = url;
		}
		public String getUrl()
		{
			return url;
		}
	}
	/**
	 * 
	 */
	public static Dataset<Row> getDataset(JavaSparkContext sc, UniDataset dataType) throws IOException {		
		switch(dataType){
			case SWISS_PROT: return getUniprotDataset(sc, dataType);
			case TREMBL: return getUniprotDataset(sc, dataType);
			case UNIREF50: return getUnirefDataset(sc, dataType);
			case UNIREF90: return getUnirefDataset(sc, dataType);
			case UNIREF100: return getUnirefDataset(sc, dataType);
			default: throw new IOException("unknown dataType: " + dataType);
		}
	}
	private static Dataset<Row> getUniprotDataset(JavaSparkContext sc, UniDataset dataType) throws IOException {
		URL u = new URL(dataType.getUrl());
		URLConnection conn = u.openConnection();
		List<Row> res = new ArrayList<Row>();
		InputStream in = conn.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
		
		String db = "", uniqueIdentifier= "", entryName= "", proteinName= "", organismName= "", geneName= "";
		int proteinExistence=0, sequenceVersion=0;
		String sequence = "", line;
		String[] tmp;
		boolean firstTime = true;
		while((line = rd.readLine()) != null)
		{
			if(line.contains(">"))
			{
				//System.out.println(line);
				if(!firstTime)
					res.add(RowFactory.create(db, uniqueIdentifier, entryName, proteinName,
							organismName, geneName, proteinExistence, sequenceVersion, sequence));
				firstTime = false;
				sequence = "";
				tmp = (line.substring(1)).split("\\|");
				db = tmp[0];
				uniqueIdentifier = tmp[1];
				tmp[0] = tmp[2];
				if(tmp[0].indexOf(" SV=") != -1)
				{
					tmp = tmp[0].split(" SV=");
					sequenceVersion = Integer.parseInt(tmp[1]);
				}
				else sequenceVersion = -1;
				if(tmp[0].indexOf(" PE=") != -1)
				{
					tmp = tmp[0].split(" PE=");
					proteinExistence = Integer.parseInt(tmp[1]);
				}
				else proteinExistence = -1;				
				if(tmp[0].indexOf(" GN=") != -1)
				{
					tmp = tmp[0].split(" GN=");
					geneName = tmp[1];
				}
				else geneName = "";	
				if(tmp[0].indexOf(" OS=") != -1)
				{
					tmp = tmp[0].split(" OS=");
					organismName = tmp[1];
				}
				else organismName = "";					
				entryName = tmp[0].split(" ")[0];
				proteinName = tmp[0].substring(tmp[0].split(" ")[0].length() + 1);
			}
			else{
				sequence = sequence.concat(line);
			}
		}
		res.add(RowFactory.create(db, uniqueIdentifier, entryName, proteinName,
				organismName, geneName, proteinExistence, sequenceVersion, sequence));
		JavaRDD<Row> data =  sc.parallelize(res);
		return JavaRDDToDataset.getDataset(data, "db", "uniqueIdentifier", "entryName",
				"proteinName", "organismName", "geneName", "proteinExistence", "sequenceVersion", "sequence");
	}
	private static Dataset<Row> getUnirefDataset(JavaSparkContext sc, UniDataset dataType) throws IOException {
		File tempFile = File.createTempFile("tmp", ".csv");
		PrintWriter pw = new PrintWriter(tempFile);
		pw.println("uniqueIdentifier,clusterName,members,taxon,taxonID,representativeMember,sequence");
		
		
		URL	u = new URL(dataType.getUrl());
		URLConnection conn = u.openConnection();
		InputStream in = conn.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
		String line, sequence = "";
		String uniqueIdentifier = "", clusterName = "", taxon = "", representativeMember = "", taxonID = "", members = "";
		boolean firstTime = true;
		String[] tmp = new String[1];
		while((line = rd.readLine()) != null)
		{
			if(line.contains(">"))
			{
				//if(uniqueIdentifier.equals("UniRef50_Q6GZW3")) break;
				//System.out.println(line);
				if(!firstTime)
					pw.println(uniqueIdentifier + "," + clusterName + "," + members + "," + taxon
							 + "," + taxonID + "," +  representativeMember + "," +  sequence);
				firstTime = false;
				sequence = "";
				tmp[0] = line.substring(1);
				if(tmp[0].split(" RepID=").length > 1)
				{
					tmp = tmp[0].split(" RepID=");
					representativeMember = tmp[1];
				}
				else representativeMember = "";
				if(tmp[0].split(" TaxID=").length > 1)
				{
					tmp = tmp[0].split(" TaxID=");
					taxonID = tmp[1];
				}
				else taxonID = "";	
				if(tmp[0].split(" Tax=").length > 1)
				{
					tmp = tmp[0].split(" Tax=");
					taxon = tmp[1];
				}
				else taxon = "";
				if(tmp[0].split(" n=").length > 1)
				{
					
					tmp = tmp[0].split(" n=");
					members = tmp[1];
				}
				else members = "";					
				uniqueIdentifier = tmp[0].split(" ")[0];
				clusterName = tmp[0].substring(tmp[0].split(" ")[0].length() + 1);
			}
			else{
				sequence = sequence.concat(line);
			}
		}
		pw.println(uniqueIdentifier + "," + clusterName + "," + members + "," + taxon
				 + "," + taxonID + "," +  representativeMember + "," +  sequence);
		pw.close();
		SparkSession spark = SparkSession
	    		.builder()
	    		.getOrCreate();
		Dataset<Row> dataset = spark.read()
	    		.format("csv")
	    		.option("header", "true")
	    		.option("inferSchema", "true")
	    		.load(tempFile.toString());
		return dataset;
		
	}
}
