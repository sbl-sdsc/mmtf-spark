package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the release date for this 
 * structure is within the specified range.
 * 
 * @author Yue Yu
 * @since 0.1.0
 *
 */

public class ReleaseDate implements Function<Tuple2<String, StructureDataInterface>, Boolean> {

	
	private static final long serialVersionUID = -3350715549847154875L;
	private String startDate;
	private String endDate;

	public ReleaseDate(String startDate, String endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
			
		return structure.getReleaseDate().compareTo(startDate) >= 0
			&& structure.getReleaseDate().compareTo(endDate) <= 0;
	}
}
