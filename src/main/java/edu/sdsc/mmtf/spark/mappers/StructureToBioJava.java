package edu.sdsc.mmtf.spark.mappers;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureReader;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.StructureDataToAdapter;

import scala.Tuple2;

/**
 * Maps a StructureDataInterface to a BioJava Structure.
 * @author Peter Rose
 *
 */
public class StructureToBioJava implements PairFunction<Tuple2<String,StructureDataInterface>,String, Structure> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Tuple2<String, Structure> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
		new StructureDataToAdapter(t._2, mmtfStructureReader);
		mmtfStructureReader.finalizeStructure();

		return new Tuple2<String, Structure>(t._1, mmtfStructureReader.getStructure());
	}
}