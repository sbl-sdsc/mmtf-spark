package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.EncoderUtils;

import scala.Tuple2;

/**
 * This filter returns entries that contain chains amino acids. The default 
 * constructor returns entries that contain at least one chain that matches 
 * the conditions. If the "exclusive" flag is set to true in the constructor, 
 * all chains must match the conditions. For a multi-model structure, this 
 * filter only checks the first model.
 * 
 * @author Peter Rose
 *
 */
public class PolymerComposition implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private boolean exclusive = false;
	private Set<String> residues;

	public static final List<String> AMINO_ACIDS_20 = Arrays.asList("ALA","ARG","ASN","ASP","CYS","GLN","GLU","GLY","HIS","ILE","LEU","LYS","MET","PHE","PRO","SER","THR","TRP","TYR","VAL");
	public static final List<String> AMINO_ACIDS_22 = Arrays.asList("ALA","ARG","ASN","ASP","CYS","GLN","GLU","GLY","HIS","ILE","LEU","LYS","MET","PHE","PRO","SER","THR","TRP","TYR","VAL","SEC","PYL");
	public static final List<String> DNA_STD_NUCLEOTIDES = Arrays.asList("DA","DC","DG","DT");
	public static final List<String> RNA_STD_NUCLEOTIDES = Arrays.asList("A","C","G","U");

	// define sets of residue types, e.g.,
	// 20 nat. amino acids, 22 nat. amino acids, nat. DNA, nat. RNA
	// custom sets
	/**
	 * The default constructor for the 20 natural amino acids
	 */
	public PolymerComposition(List<String> groupNames) {
	    this.residues = new HashSet<String>(groupNames);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively match all chains.
	 * @param exclusive if true, all chains must match contain the 20 natural L-amino acids.
	 */
	public PolymerComposition(boolean exclusive, List<String> groupNames) {
		this.exclusive = exclusive;
		this.residues = new HashSet<String>(groupNames);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively match all chains.
	 * @param exclusive if true, all chains must match the specified group names (uppercase).
	 */
	public PolymerComposition(boolean exclusive, String...groupNames) {
		this.exclusive = exclusive;
		this.residues = new HashSet<>(Arrays.asList(groupNames));
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		boolean containsPolymer = false;
		boolean globalMatch = false;
		int numChains = structure.getChainsPerModel()[0]; // only check first model

		for (int i = 0, groupCounter = 0; i < numChains; i++){
			boolean match = true;	
			String chainType = EncoderUtils.getTypeFromChainId(structure, i);
			boolean polymer = chainType.equals("polymer");

			if (polymer) {
				containsPolymer = true;
			} else {
				match = false;
			}

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++) {			
				if (match && polymer) {
					int groupIndex = structure.getGroupTypeIndices()[groupCounter];
					String name = structure.getGroupName(groupIndex);
					match = residues.contains(name);
				}
			}

			if (polymer && match && ! exclusive) {
				return true;
			}
			if (polymer && ! match && exclusive) {
				return false;
			}
			
			if (match) {
				globalMatch = true;
			}
		}
			
		return globalMatch && containsPolymer;
	}
}
