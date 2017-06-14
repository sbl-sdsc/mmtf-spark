package edu.sdsc.mmtf.spark.mappers;

import org.apache.spark.api.java.function.Function;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureReader;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.StructureDataToAdapter;

/**
 * Maps a StructureDataInterface to a BioJava Structure.
 * @author Peter Rose
 *
 */
public class StructureToBioJava implements Function<StructureDataInterface, Structure> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Structure call(StructureDataInterface t) throws Exception {
		MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
		try {
	
		new StructureDataToAdapter(t, mmtfStructureReader);


		} catch (Exception e) {
			System.out.println("Exception: " + t.getStructureId());
			e.printStackTrace();
		}
		return mmtfStructureReader.getStructure();
	}
}