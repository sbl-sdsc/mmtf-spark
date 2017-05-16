package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter passes entries that contain RNA chains. The default constructor 
 * passes entries that contain at least one RNA chain. If the "exclusive" flag is 
 * set to true in the constructor, all polymer chains must be RNA. For a multi-model 
 * structure (e.g., NMR structure), this filter only checks the first model.
 * 
 * @author Peter Rose
 * 
 *
 */
public class ContainsRnaChain implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private ContainsPolymerType filter;

	/**
	 * Default constructor matches any entry that contains at least one RNA chain.
	 * As an example, a RNA-protein complex passes this filter.
	 */
	public ContainsRnaChain() {
		this(false);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively contain DNA chains.
	 * For example, with "exclusive" set to true, a RNA-protein complex complex does not pass this filter.
	 * @param exclusive if true, only return entries that contain RNA chains
	 */
	public ContainsRnaChain(boolean exclusive) {
		this.filter = new ContainsPolymerType(exclusive, "RNA LINKING");
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
