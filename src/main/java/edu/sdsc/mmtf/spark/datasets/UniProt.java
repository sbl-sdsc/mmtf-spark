package edu.sdsc.mmtf.spark.datasets;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class <a href="http://www.uniprot.org/downloads">downloads</a> and reads
 * UniProt sequence files in the FASTA format and converts them to datasets. This
 * class reads the following files: SWISS_PROT, TREMBL, UNIREF50, UNIREF90, UNIREF100.
 * 
 * The datasets have the following
 * <a href="http://www.uniprot.org/help/fasta-headers">columns</a>.
 * 
 * <p>
 * Example: download, read, and save the SWISS_PROT dataset
 * 
 * <pre>
 * 	{@code
 * 	Dataset<Row> ds = UniProt.getDataset(UniProtDataset.SWISS_PROT);
 * 	ds.printSchema();
 * 	ds.show(5);
 *  ds.write().mode("overwrite").format("parquet").save(fileName);
 *  }
 * </pre>
 * 
 * @author Yue Yu
 * 
 */
public class UniProt {
	private static String baseUrl = "ftp://ftp.uniprot.org/pub/databases/uniprot/";
	
	public enum UniProtDataset {
		SWISS_PROT(baseUrl + "current_release/knowledgebase/complete/uniprot_sprot.fasta.gz"), 
		TREMBL(baseUrl + "current_release/knowledgebase/complete/uniprot_trembl.fasta.gz"), 
		UNIREF50(baseUrl + "uniref/uniref50/uniref50.fasta.gz"), 
		UNIREF90(baseUrl + "uniref/uniref90/uniref90.fasta.gz"), 
		UNIREF100(baseUrl + "uniref/uniref100/uniref100.fasta.gz");
	
		private final String url;

		UniProtDataset(String url) {
			this.url = url;
		}

		public String getUrl() {
			return url;
		}
	}
	
	/**
	 * Returns the specified UniProt dataset.
	 * @param uniProtDataset name of the UniProt dataset
	 * @return dataset with sequence and metadata
	 * @throws IOException
	 */
	public static Dataset<Row> getDataset(UniProtDataset uniProtDataset) throws IOException {
		switch (uniProtDataset) {
		case SWISS_PROT:
			return getUniprotDataset(uniProtDataset);
		case TREMBL:
			return getUniprotDataset(uniProtDataset);
		case UNIREF50:
			return getUnirefDataset(uniProtDataset);
		case UNIREF90:
			return getUnirefDataset(uniProtDataset);
		case UNIREF100:
			return getUnirefDataset(uniProtDataset);
		default:
			throw new IllegalArgumentException("unknown dataset: " + uniProtDataset);
		}
	}

	private static Dataset<Row> getUniprotDataset(UniProtDataset dataType) throws IOException {
		File tempFile = File.createTempFile("tmp", ".csv");
		PrintWriter pw = new PrintWriter(tempFile);

		pw.println("db,uniqueIdentifier,entryName,proteinName,organismName,geneName,"
				+ "proteinExistence,sequenceVersion,sequence");

		URL u = new URL(dataType.getUrl());
		InputStream in = u.openStream();

		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));

		String db = "", uniqueIdentifier = "", entryName = "", proteinName = "", organismName = "", geneName = "",
				proteinExistence = "", sequenceVersion = "", sequence = "", line;

		String[] tmp;
		boolean firstTime = true;

		while ((line = rd.readLine()) != null) {
			if (line.contains(">")) {
				line = line.replace(",", ";");
				if (!firstTime) {
					pw.println(db + "," + uniqueIdentifier + "," + entryName + "," + proteinName + "," + organismName
							+ "," + geneName + "," + proteinExistence + "," + sequenceVersion + "," + sequence);
				}
			    firstTime = false;
				sequence = "";
				tmp = (line.substring(1)).split("\\|");
				db = tmp[0];
				uniqueIdentifier = tmp[1];
				tmp[0] = tmp[2];

				if (tmp[0].split(" OS=").length > 2) {
					tmp[0] = tmp[0].substring(0, tmp[0].split(" OS=")[0].length() + tmp[0].split(" OS=")[1].length() + 4);
				}
				tmp = tmp[0].split(" SV=", -1);
				sequenceVersion = tmp.length > 1 ? tmp[1] : "";
				tmp = tmp[0].split(" PE=", -1);
				proteinExistence = tmp.length > 1 ? tmp[1] : "";
				tmp = tmp[0].split(" GN=", -1);
				geneName = tmp.length > 1 ? tmp[1] : "";
				tmp = tmp[0].split(" OS=", -1);
				organismName = tmp.length > 1 ? tmp[1] : "";
				entryName = tmp[0].split(" ")[0];
				proteinName = tmp[0].substring(tmp[0].split(" ")[0].length() + 1);
			} else {
				sequence = sequence.concat(line);
			}
		}
		pw.println(db + "," + uniqueIdentifier + "," + entryName + "," + proteinName + "," + organismName + ","
				+ geneName + "," + proteinExistence + "," + sequenceVersion + "," + sequence);
		
		pw.close();
		rd.close();

		SparkSession spark = SparkSession.builder().getOrCreate();

		Dataset<Row> dataset = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				.load(tempFile.toString());

		return dataset;
	}

	private static Dataset<Row> getUnirefDataset(UniProtDataset dataType) throws IOException {
		File tempFile = File.createTempFile("tmp", ".csv");
		PrintWriter pw = new PrintWriter(tempFile);
		pw.println("uniqueIdentifier,clusterName,members,taxon,taxonID,representativeMember,sequence");

		URL u = new URL(dataType.getUrl());
		InputStream in = u.openStream();

		BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));

		String line, sequence = "";
		String uniqueIdentifier = "", clusterName = "", taxon = "", representativeMember = "", taxonID = "",
				members = "";
		boolean firstTime = true;
		String[] tmp = new String[1];

		while ((line = rd.readLine()) != null) {
			if (line.contains(">")) {
				line = line.replace(",", ";");
				if (!firstTime) {
					pw.println(uniqueIdentifier + "," + clusterName + "," + members + "," + taxon + "," + taxonID + ","
							+ representativeMember + "," + sequence);
				}
				firstTime = false;
				
				sequence = "";
				tmp[0] = line.substring(1);
				tmp = tmp[0].split(" RepID=", -1);
				representativeMember = tmp.length > 1 ? tmp[1] : "";
				tmp = tmp[0].split(" TaxID=", -1);
				taxonID = tmp.length > 1 ? tmp[1] : "";
				tmp = tmp[0].split(" Tax=", -1);
				taxon = tmp.length > 1 ? tmp[1] : "";
				tmp = tmp[0].split(" n=", -1);
				members = tmp.length > 1 ? tmp[1] : "";
				uniqueIdentifier = tmp[0].split(" ")[0];
				clusterName = tmp[0].substring(tmp[0].split(" ")[0].length() + 1);
			} else {
				sequence = sequence.concat(line);
			}
		}
		pw.println(uniqueIdentifier + "," + clusterName + "," + members + "," + taxon + "," + taxonID + ","
				+ representativeMember + "," + sequence);
		
		pw.close();
		rd.close();

		SparkSession spark = SparkSession.builder().getOrCreate();

		Dataset<Row> dataset = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				.load(tempFile.toString());
		
		return dataset;
	}
}
