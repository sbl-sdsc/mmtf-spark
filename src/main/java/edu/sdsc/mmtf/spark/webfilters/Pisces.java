package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.webservices.PiscesDownloader;
import scala.Tuple2;

/**
 * This filter passes through representative structures or protein chains 
 * from the PISCES CulledPDB sets. A CulledPDB set is selected by specifying
 * sequenceIdentity and resolution cutoff values from the following
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
 */
public class Pisces implements Function<Tuple2<String, StructureDataInterface>, Boolean> {	
	private static final long serialVersionUID = -3962877268210540994L;
	private Set<String> pdbIds;

	/**
	 * Filters representative PDB structures and polymer chains based
	 * on the specified criteria using PISCES CulledPDB sets.
	 * <p> sequenceIdentity = 20, 25, 30, 40, 50, 60, 70, 80, 90
	 * <p> resolution = 1.6, 1.8, 2.0, 2.2, 2.5, 3.0
	 * 
	 * @param sequenceIdentity sequence identity cutoff value
	 * @param resolution resolution cutoff value
	 * @throws IOException if data set cannot be downloaded from PISCES server.
	 */
	public Pisces(int sequenceIdentity, double resolution) throws IOException {
		pdbIds = new HashSet<>();
		
		PiscesDownloader pD = new PiscesDownloader(sequenceIdentity, resolution);
		for (String pdbId: pD.getStructureChainIds()) {
			pdbIds.add(pdbId); // add PDB ID.ChainId
			pdbIds.add(pdbId.substring(0,4)); // add PDB ID
		}
	}
		
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) {
		return pdbIds.contains(t._1);
	}
}
