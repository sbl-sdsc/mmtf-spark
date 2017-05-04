package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns true if the StructureDataInterface contains a single protein chain of L-amino acids.
 * @author Peter Rose
 *
 */
public class IsLProteinChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		// this filter passes only single chains and the sequence cannot be empty
		if (structure.getNumEntities() != 1 || structure.getEntitySequence(0).isEmpty()) {
			return false;			
		} 
			
		for (int index: structure.getGroupTypeIndices()) {
			String type = structure.getGroupChemCompType(index);
			if ( !(type.equals("L-PEPTIDE LINKING") || type.equals("PEPTIDE LINKING")) ) {
				return false;
			}
		}
			
		return true;
	}
}
