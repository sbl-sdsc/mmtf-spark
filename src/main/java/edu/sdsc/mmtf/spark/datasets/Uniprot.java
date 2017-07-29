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
	private static final String URL = "ftp://ftp.uniprot.org/pub/databases/"
			+ "uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz";
	
	/**
	 * TODO
	 */
	public static Dataset<Row> getDataset(JavaSparkContext sc) throws IOException {	

		URL u = new URL(URL);
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
			//System.out.println(line);
			if(line.contains(">"))
			{
				if(!firstTime)
					res.add(RowFactory.create(db, uniqueIdentifier, entryName, proteinName, organismName, geneName, proteinExistence, sequenceVersion, sequence));
				firstTime = false;
				sequence = "";
				tmp = (line.substring(1)).split("\\|");
				db = tmp[0];
				uniqueIdentifier = tmp[1];
				tmp = tmp[2].split(" OS=");
				entryName = tmp[0].split(" ")[0];
				proteinName = tmp[0].substring(tmp[0].split(" ")[0].length());
				tmp = tmp[1].split(" GN=");
				if(tmp.length == 1)
				{
					tmp = tmp[0].split(" PE=");
					organismName = tmp[0];
					geneName = "null";
				}
				else{
					organismName = tmp[0];
					tmp = tmp[1].split(" PE=");
					geneName = tmp[0];
				}
				tmp = tmp[1].split(" SV=");
				proteinExistence = Integer.parseInt(tmp[0]);
				sequenceVersion = Integer.parseInt(tmp[1]);
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
	
}
