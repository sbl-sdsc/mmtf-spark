package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if the r-work value for this structure is within the specified range.
 * 
 * @see <a href="https://pdb101.rcsb.org/learn/guide-to-understanding-pdb-data/r-value-and-r-free">rvalue</a>
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class Rwork implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -5470014873227534021L;
	private double minRwork;
	private double maxRwork;
	
	public Rwork(double minRwork, double maxRwork) {
		this.minRwork = minRwork;
		this.maxRwork = maxRwork;
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		return structure.getRwork() >= minRwork && structure.getRwork() <= maxRwork;
	}
}
