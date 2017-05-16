package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Maps a structure to its unique polymer sequences.
 * @author Peter Rose
 *
 */
public class StructureToPolymerSequences implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, String> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Iterator<Tuple2<String, String>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		List<Tuple2<String, String>> sequences = new ArrayList<>();

		for (int i = 0; i < structure.getNumEntities(); i++) {
			if (structure.getEntityType(i).equals("polymer") && ! structure.getEntitySequence(i).isEmpty()) {
				sequences.add(new Tuple2<String,String>(t._1, structure.getEntitySequence(i)));
			}
		}

		return sequences.iterator();
	}
}