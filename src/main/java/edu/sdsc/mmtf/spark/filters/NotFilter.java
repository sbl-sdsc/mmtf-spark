package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter wraps another filter and negates its result.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class NotFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private Function<Tuple2<String, StructureDataInterface>, Boolean> filter;

    /**
     * Constructor takes another filter as input
     * @param filter Filter to be negated
     */
	public NotFilter(Function<Tuple2<String, StructureDataInterface>, Boolean> filter) {
		this.filter = filter;
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return !filter.call(t);
	}
}
