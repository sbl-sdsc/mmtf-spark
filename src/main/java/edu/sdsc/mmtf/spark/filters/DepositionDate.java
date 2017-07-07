package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the deposition date for this 
 * structure is within the specified range.
 * 
 * @author Yue Yu
 *
 */

public class DepositionDate implements Function<Tuple2<String, StructureDataInterface>, Boolean> {

	private static final long serialVersionUID = -2352226599988882246L;
	private String startDate;
	private String endDate;

	public DepositionDate(String startDate, String endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		return structure.getDepositionDate().compareTo(startDate) >= 0
			&& structure.getDepositionDate().compareTo(endDate) <= 0;
	}
}
