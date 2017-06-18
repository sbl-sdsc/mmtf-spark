package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter wraps two filter and returns true if
 * one of the filters passes.
 * 
 * @author Peter Rose
 *
 */
public class OrFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private Function<Tuple2<String, StructureDataInterface>, Boolean> filter1;
	private Function<Tuple2<String, StructureDataInterface>, Boolean> filter2;
    /**
     * Constructor takes another filter as input
     * @param filter Filter to be negated
     */
	public OrFilter(Function<Tuple2<String, StructureDataInterface>, Boolean> filter1, Function<Tuple2<String, StructureDataInterface>, Boolean> filter2) {
		this.filter1 = filter1;
		this.filter2 = filter2;
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter1.call(t) || filter2.call(t);
	}
}
