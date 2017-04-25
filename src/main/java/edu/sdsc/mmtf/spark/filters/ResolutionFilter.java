package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the resolution value for this structure is within the specified range.
 * @author Peter Rose
 *
 */
public class ResolutionFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	private double minResolution;
	private double maxResolution;
	
	public ResolutionFilter(double minResolution, double maxResolution) {
		this.minResolution = minResolution;
		this.maxResolution = maxResolution;
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		if (structure.getResolution() >= minResolution && structure.getResolution() <= maxResolution) {
			return true;
		} else {
			return false;		
		}
	}
}
