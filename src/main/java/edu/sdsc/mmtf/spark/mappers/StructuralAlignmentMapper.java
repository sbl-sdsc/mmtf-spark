package edu.sdsc.mmtf.spark.mappers;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import edu.sdsc.mmtf.spark.utils.BiojavaAligner;
import edu.sdsc.mmtf.spark.utils.ExhaustiveAligner;
import scala.Tuple2;

/**
 * Flatmap function used for the calculations of structural alignments of protein chains. 
 * This class use a broadcasted list of protein coordinates for efficient processing.
 * The input to the call method are two indices for two protein chain into the broadcasted list.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class StructuralAlignmentMapper implements FlatMapFunction<Tuple2<Integer, Integer>, Row> {
	private static final long serialVersionUID = -3505435321631952639L;

	private String alignmentAlgorithm;
	private Broadcast<List<Tuple2<String, Point3d[]>>> chains = null;

	public StructuralAlignmentMapper(Broadcast<List<Tuple2<String, Point3d[]>>> chains, String alignmentAlgorithm) {
		this.chains = chains;
		this.alignmentAlgorithm = alignmentAlgorithm;
	}

	@Override
	public Iterator<Row> call(Tuple2<Integer, Integer> pair) throws Exception {		
		int xIndex = pair._1();
		int yIndex = pair._2();
		
		// get list from broadcasted object
		List<Tuple2<String, Point3d[]>> data = this.chains.getValue();
		
		// get chain names
		String xId = data.get(xIndex)._1();
		String yId = data.get(yIndex)._1();
		

		// get coordinate arrays
	    Point3d[] x = data.get(xIndex)._2();
	    Point3d[] y = data.get(yIndex)._2();
		    
	    // establish unique order base on the Ids
		if (xId.compareTo(yId) > 0) {
		    String s = xId;
		    xId = yId;
		    yId = s;
		    Point3d[] p = x;
		    x = y;
		    y = p;
		}

		// create a unique key
		String key = xId + "-" + yId;
		
		List<Row> rows = Collections.emptyList();
		
		// run the alignments
		if (BiojavaAligner.isSupportedAlgorithm(alignmentAlgorithm)) {
		    rows = BiojavaAligner.getAlignment(alignmentAlgorithm, key, x, y); // returns a single alignment
		    
		} else if (ExhaustiveAligner.isSupportedAlgorithm(alignmentAlgorithm)) {
			ExhaustiveAligner aligner = new ExhaustiveAligner();
			aligner.setMinLength(0);
			aligner.setMinCoverage(0);
			aligner.setMaxRmsd(1000.0);
			aligner.setMinTm(0.1);
			rows = aligner.getAlignments(alignmentAlgorithm, key, x, y); // returns zero or more alignments
		}

		return rows.iterator();
	}
}