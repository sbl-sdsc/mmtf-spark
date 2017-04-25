package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;
import scala.Tuple2;

/**
 * This filter return true if the StructureDataInterface contains a single DNA.
 * @author Peter Rose
 *
 */
public class IsLDnaChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	private static Set<Character> oneLetterCode = new HashSet<>(Arrays.asList(new Character[]{'A','C','G','T','X'}));

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		if (structure.getNumEntities() > 1) {
			// this filter passes only single chains
			return false;
			
		} else if (structure.getNumEntities() == 1) {
			// non-polymers have no sequence
			if (structure.getEntitySequence(0).length() == 0) {
				return false;
			}
			for (int index: structure.getGroupTypeIndices()) {
		     	String type = structure.getGroupChemCompType(index);
		     	if ( !(type.equals("L-DNA LINKING") || type.equals("DNA LINKING")) ) {
		     		return false;
		     	}
			}
			// check against the amino acid alphabet
			for (Character c: structure.getEntitySequence(0).toCharArray()) {
				if (!oneLetterCode.contains(c)) {
					return false;
				}
			}
			
		} else if (structure.getNumEntities() == 0) {
            // entity info is optional. If entity information is not available, check the one-letter code for each group instead
			for (int index: structure.getGroupTypeIndices()) {
			   if (!oneLetterCode.contains(structure.getGroupSingleLetterCode(index))) {
				   return false;
			   }
			}
		}

		return true;
	}
}
