package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that contain chain(s) made of D-saccharides. 
 * The default constructor returns entries that contain at least one 
 * polymer chain that is an L-protein. If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must be L-proteins. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * @author Peter Rose
 *
 */
public class ContainsDSaccharide implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -2323293283758321260L;
	private ContainsPolymerType filter;

	/**
	 * Default constructor matches any entry that contains at least one chain made of D-saccharides.
	 * As an example, a glycosylated protein complex passes this filter.
	 */
	public ContainsDSaccharide() {
		this(false);
	}
	
	/**
	 * Optional constructor that can be used to filter entries that exclusively contain L-protein chains.
	 * For example, with "exclusive" set to true, an L-protein/L-DNA complex does not pass this filter.
	 * @param exclusive if true, only return entries that are exclusively contain L-protein chains
	 */
	public ContainsDSaccharide(boolean exclusive) {
		this.filter = new ContainsPolymerType(exclusive, "D-SACCHARIDE", "SACCHARIDE",
				"D-SACCHARIDE 1,4 AND 1,4 LINKING","D-SACCHARIDE 1,4 AND 1,6 LINKING");
	}
	
	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return filter.call(t);
	}
}
