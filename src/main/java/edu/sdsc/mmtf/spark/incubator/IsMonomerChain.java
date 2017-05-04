package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the StructureDataInterface contains a single protein chain. A single chains that contains one of the standard 20 amino acids,
 * Pyrrolysine, Selenocysteine, and unknown amino acids.
 * @author Peter Rose
 *
 */
public class IsMonomerChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		if (structure.getNumEntities() > 1) {
			// this filter passes only single chains
			return false;
			
		} else if (structure.getNumEntities() == 1) {
			// non-polymers have no sequence
			if (structure.getEntitySequence(0).length() > 0) {
				return false;
			}	
		} 

		return true;
	}
}
