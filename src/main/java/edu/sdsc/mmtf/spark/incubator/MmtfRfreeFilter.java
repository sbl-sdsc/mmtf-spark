package edu.sdsc.mmtf.spark.incubator;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.dataholders.MmtfStructure;

import scala.Tuple2;

/**
 * This filter return true if the rFree value for this structure is within the specified range.
 * @author Peter Rose
 *
 */
public class MmtfRfreeFilter implements Function<Tuple2<String, MmtfStructure>, Boolean> {
	private static final long serialVersionUID = 2881046310099057240L;
	private double minRfree;
	private double maxRfree;
	
	public MmtfRfreeFilter(double minRfree, double maxRfree) {
		this.minRfree = minRfree;
		this.maxRfree = maxRfree;
	}

	@Override
	public Boolean call(Tuple2<String, MmtfStructure> t) throws Exception {
		MmtfStructure structure = t._2;
		
		Float rFree = structure.getrFree();
		if (rFree == null) return false;
	
		return structure.getrFree() >= minRfree && structure.getrFree() <= maxRfree;
	}
}
