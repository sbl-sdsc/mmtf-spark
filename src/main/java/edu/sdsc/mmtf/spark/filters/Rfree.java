package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the rFree value for this structure is within the specified range.
 * @see <a href="http://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free">rfree</a>
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class Rfree implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = 2881046310099057240L;
	private double minRfree;
	private double maxRfree;
	
	public Rfree(double minRfree, double maxRfree) {
		this.minRfree = minRfree;
		this.maxRfree = maxRfree;
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		return structure.getRfree() >= minRfree && structure.getRfree() <= maxRfree;
	}
}
