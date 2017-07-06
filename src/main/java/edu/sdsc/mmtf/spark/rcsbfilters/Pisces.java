package edu.sdsc.mmtf.spark.rcsbfilters;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.PiscesDownload;
import scala.Tuple2;

/**
 * 
 * This filter //TODO 
 * 
 * For details see 
 * <a href="http://dunbrack.fccc.edu/Guoli/pisces_download.php">PISCES</a>.
 * 
 * @author Yue Yu
 */
public class Pisces implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	
	private static final long serialVersionUID = -4794067375376198086L;
	private Set<String> pdbIds;

	
	/**
	 * Filters representative PDB structures 
	 * @param whereClause WHERE Clause of SQL statement
	 * @param fields one or more field names to be used in query
	 * @throws IOException
	 */
	public Pisces(int sequenceIdentity, double resolution, double rFactor) throws IOException {

		pdbIds = new HashSet<>();
		//System.out.println("begin download");
		PiscesDownload pD = new PiscesDownload(sequenceIdentity, resolution, rFactor);
		for (String pdbId: pD.getPdbIds()) {
			pdbIds.add(pdbId); // add PDB ID.ChainId
			pdbIds.add(pdbId.substring(0,4)); // add PDB ID
		}
	}
	
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) {
		return pdbIds.contains(t._1);
	}
}
