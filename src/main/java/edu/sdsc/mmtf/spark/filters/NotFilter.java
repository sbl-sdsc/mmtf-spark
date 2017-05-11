package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns 
 * 
 * @author Peter Rose
 *
 */
public class NotFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private Function<Tuple2<String, StructureDataInterface>, Boolean> filter;

	/**
	 */
	public NotFilter(Function<Tuple2<String, StructureDataInterface>, Boolean> filter) {
		this.filter = filter;
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return !filter.call(t);
	}
}
