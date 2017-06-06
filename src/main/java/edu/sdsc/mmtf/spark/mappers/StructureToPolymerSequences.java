package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Maps a structure to its polypeptides, polynucleotides chain sequences. 
 * For a multi-model structure, only the first model is considered.
 * 
 * @author Peter Rose
 */
public class StructureToPolymerSequences implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, String> {
	private static final long serialVersionUID = 8907755561668400639L;
	private boolean useChainIdInsteadOfChainName = false;
	private boolean removeDuplicates = false;

	/**
	 * Extracts all polymer chains from a structure. A key is assigned to
	 * each polymer: <PDB ID.Chain Name>, e.g., 4HHB.A. Here Chain 
	 * Name is the name of the chain as found in the corresponding pdb file.
	 */
	public StructureToPolymerSequences() {}
	
	/**
	 * Extracts all polymer chains from a structure. If the argument is set to true,
	 * the assigned key is: <PDB ID.Chain ID>, where Chain ID is the unique identifier
	 * assigned to each molecular entity in an mmCIF file. This Chain ID corresponds to
	 * <a href="http://mmcif.wwpdb.org/dictionaries/mmcif_mdb.dic/Items/_atom_site.label_asym_id.html">
	 * _atom_site.label_asym_id</a> field in an mmCIF file.
	 * @param useChainIdInsteadOfChainName if true, use the Chain Id in the key assignments
	 * @param removeDuplicates if true, return only one chain for each unique sequence
	 */
	public StructureToPolymerSequences(boolean useChainIdInsteadOfChainName, boolean duplicateSequences) {
		this.useChainIdInsteadOfChainName = useChainIdInsteadOfChainName;
		this.removeDuplicates = duplicateSequences;
	}
	
	@Override
	public Iterator<Tuple2<String, String>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
		List<Tuple2<String, String>> sequences = new ArrayList<>();
		Set<String> seqSet = new HashSet<>();

		// precalculate indices
		int[] chainToEntityIndex = getChainToEntityIndex(structure);
		
		for (int i = 0; i < structure.getChainsPerModel()[0]; i++){	
			boolean polymer = structure.getEntityType(chainToEntityIndex[i]).equals("polymer");

			if (polymer) {
				String key = t._1;
				
				// remove any previous chain id information
				if (key.contains(".")){
					key = key.substring(0, key.indexOf("."));
				}
				
				key += ".";
				if (useChainIdInsteadOfChainName) {
					key += structure.getChainIds()[i];
				} else {
					key += structure.getChainNames()[i];
				}

				if (removeDuplicates) {
					if (seqSet.contains(structure.getEntitySequence(chainToEntityIndex[i]))) {
						continue;
					}
	                seqSet.add(structure.getEntitySequence(chainToEntityIndex[i]));
				}
				
				sequences.add(new Tuple2<String,String>(key, structure.getEntitySequence(chainToEntityIndex[i])));
			}
		}

		return sequences.iterator();
	}

	/**
	 * Returns an array that maps a chain index to an entity index.
	 * @param structureDataInterface
	 * @return
	 */
	private static int[] getChainToEntityIndex(StructureDataInterface structure) {
		int[] entityChainIndex = new int[structure.getNumChains()];

		for (int i = 0; i < structure.getNumEntities(); i++) {
			for (int j: structure.getEntityChainIndexList(i)) {
				entityChainIndex[j] = i;
			}
		}
		return entityChainIndex;
	}
}