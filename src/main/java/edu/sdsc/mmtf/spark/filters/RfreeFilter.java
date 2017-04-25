package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the rFree value for this structure is within the specified range.
 * @author Peter Rose
 *
 */
public class RfreeFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	private double minRfree;
	private double maxRfree;
	
	public RfreeFilter(double minRfree, double maxRfree) {
		this.minRfree = minRfree;
		this.maxRfree = maxRfree;
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		if (structure.getRfree() >= minRfree && structure.getRfree() <= maxRfree) {
			return true;
		} else {
			return false;		
		}
	}
}
