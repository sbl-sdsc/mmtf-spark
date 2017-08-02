package edu.sdsc.mmtf.spark.datasets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * @author Yue Yu
 * 
 */
public class Uniprot {
	private static final String Swiss_Prot_URL = "ftp://ftp.uniprot.org/pub/databases/"
			+ "uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz";
	private static final String TreMBL_URL = "ftp://ftp.uniprot.org/pub/databases/"
			+ "uniprot/current_release/knowledgebase/complete/uniprot_trembl.fasta.gz";
	private static final String UniRef50_URL = "ftp://ftp.uniprot.org/pub/databases/"
			+ "uniprot/uniref/uniref50/uniref50.fasta.gz";
	
	/**
	 * TODO
	 */
	public static Dataset<Row> getDataset(JavaSparkContext sc, String dataType) throws IOException {	
		URL u = null;
		if(dataType.equals("swiss_prot"))
		{
			u = new URL(Swiss_Prot_URL);
		}
		if(dataType.equals("TreMBL"))
		{
			u = new URL(TreMBL_URL);
		}
		if(u != null)
		{
			URLConnection conn = u.openConnection();
			List<Row> res = new ArrayList<Row>();
			InputStream in = conn.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
			
			String db = "", uniqueIdentifier= "", entryName= "", proteinName= "", organismName= "", geneName= "";
			int proteinExistence=0, sequenceVersion=0;
			String sequence = "";
			String line;
			String[] tmp;
			boolean firstTime = true;
			while((line = rd.readLine()) != null)
			{
				if(line.contains(">"))
				{
					//System.out.println(line);
					if(!firstTime)
						res.add(RowFactory.create(db, uniqueIdentifier, entryName, proteinName, organismName, geneName, proteinExistence, sequenceVersion, sequence));
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
					else geneName = "null";	
					if(tmp[0].indexOf(" OS=") != -1)
					{
						tmp = tmp[0].split(" OS=");
						organismName = tmp[1];
					}
					else organismName = "null";					
					
					entryName = tmp[0].split(" ")[0];
					proteinName = tmp[0].substring(tmp[0].split(" ")[0].length() + 1);
	//				
	//				System.out.println("db is : " + db + "\nUniqueId is : " + uniqueIdentifier
	//						+ "\nEntryName is : " + entryName + "\nproteinName is : " + proteinName
	//						+ "\norganismName is : " + organismName + "\ngeneName is : " + geneName
	//						+ "\nproteinExistence is : " + proteinExistence + "\nsequenceVersion is : " + sequenceVersion);
				}
				else{
					sequence = sequence.concat(line);
				}
			}
			
			
			JavaRDD<Row> data =  sc.parallelize(res);
			
			return JavaRDDToDataset.getDataset(data, "db", "uniqueIdentifier", "entryName",
					"proteinName", "organismName", "geneName", "proteinExistence", "sequenceVersion", "sequence");
		}
		//Uniref
		else
		{
			if(dataType.equals("UniRef50"))
				u = new URL(UniRef50_URL);
			URLConnection conn = u.openConnection();
			List<Row> res = new ArrayList<Row>();
			InputStream in = conn.getInputStream();
			BufferedReader rd = new BufferedReader(new InputStreamReader(new GZIPInputStream(in)));
			String line;
			
			String uniqueIdentifier = "", clusterName = "", taxon = "", representativeMember = "";
			int taxonID = -1, members = -1;
			boolean firstTime = true;
			String sequence = "";
			String[] tmp = new String[1];
			while((line = rd.readLine()) != null)
			{
				if(line.contains(">"))
				{
					System.out.println(line);
					if(!firstTime)
						res.add(RowFactory.create(uniqueIdentifier, clusterName, members, taxon, taxonID, representativeMember, sequence));
					firstTime = false;
					sequence = "";
					tmp[0] = line.substring(1);
					if(tmp[0].indexOf(" RepID=") != -1)
					{
						tmp = tmp[0].split(" RepID=");
						representativeMember = tmp[1];
					}
					else representativeMember = "null";
					if(tmp[0].indexOf(" TaxID=") != -1)
					{
						tmp = tmp[0].split(" TaxID=");
						taxonID = Integer.parseInt(tmp[1]);
					}
					else taxonID = -1;	
					if(tmp[0].indexOf(" Tax=") != -1)
					{
						tmp = tmp[0].split(" Tax=");
						taxon = tmp[1];
					}
					else taxon = "null";
				
					if(tmp[0].indexOf(" n=") != -1)
					{
						
						tmp = tmp[0].split(" n=");
						members = Integer.parseInt(tmp[1]);
					}
					else members = -1;					
					
					uniqueIdentifier = tmp[0].split(" ")[0];
					clusterName = tmp[0].substring(tmp[0].split(" ")[0].length() + 1);
				}
				else{
					sequence = sequence.concat(line);
				}
			}
			JavaRDD<Row> data = null;
			
			return JavaRDDToDataset.getDataset(data, "uniqueIdentifier", "clusterName", "members",
					"taxon", "taxonID", "representativeMember", "sequence");
		}
	}
	
}
