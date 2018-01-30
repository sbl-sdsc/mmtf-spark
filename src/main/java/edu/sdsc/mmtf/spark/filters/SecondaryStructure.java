package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.EncoderUtils;

import edu.sdsc.mmtf.spark.utils.DsspSecondaryStructure;
import scala.Tuple2;

/**
 * This filter returns entries that contain polymer chain(s) with the specified fraction of
 * secondary structure assignments, obtained by DSSP. Note, DSSP secondary structure
 * in MMTF files is assigned by the BioJava implementation of DSSP. It may differ in some 
 * cases from the original DSSP implementation.
 * 
 * The default constructor returns entries that contain at least one 
 * polymer chain that matches the criteria. If the "exclusive" flag is set to true 
 * in the constructor, all polymer chains must match the criteria. For a multi-model structure,
 * this filter only checks the first model.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class SecondaryStructure implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	double helixFractionMin = 0;
	double helixFractionMax = 1.0;
	double sheetFractionMin = 0;
	double sheetFractionMax = 1.0;
	double coilFractionMin = 0;
	double coilFractionMax = 1.0;
	boolean exclusive = false;

	public SecondaryStructure(double helixFractionMin, double helixFractionMax, 
			double sheetFractionMin, double sheetFractionMax,
			double coilFractionMin, double coilFractionMax) {
		this(helixFractionMin, helixFractionMax, 
				sheetFractionMin, sheetFractionMax,
				coilFractionMin, coilFractionMax, false);
	}

	public SecondaryStructure(double helixFractionMin, double helixFractionMax, 
			double sheetFractionMin, double sheetFractionMax,
			double coilFractionMin, double coilFractionMax, boolean exclusive) {
		this.helixFractionMin = helixFractionMin;
		this.helixFractionMax = helixFractionMax;
		this.sheetFractionMin = sheetFractionMin;
		this.sheetFractionMax = sheetFractionMax;
		this.coilFractionMin = coilFractionMin;
		this.coilFractionMax = coilFractionMax;
		this.exclusive = exclusive;
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		boolean containsPolymer = false;
		boolean globalMatch = false;
		int numChains = structure.getChainsPerModel()[0]; // only check first model
		int[] secStruct = structure.getSecStructList();	


		for (int i = 0, groupCounter = 0; i < numChains; i++) {		
			double helix = 0;
			double sheet = 0;
			double coil = 0;
			int other = 0;

			boolean match = true;	
			String chainType = EncoderUtils.getTypeFromChainId(structure, i);
			boolean polymer = chainType.equals("polymer");

			if (polymer) {
				containsPolymer = true;
			} else {
				match = false;
			}

			for (int j = 0; j < structure.getGroupsPerChain()[i]; j++, groupCounter++) {	

				if (match && polymer) {
					int code = secStruct[groupCounter];
					switch (DsspSecondaryStructure.getQ3Code(code)) {

					case ALPHA_HELIX:
						helix++;
						break;
					case EXTENDED:
						sheet++;
						break;
					case COIL:
						coil++;
						break;
					default:
						other++;
						break;
					}
				}
			}

			if (match && polymer) {
				int n = (structure.getGroupsPerChain()[i] - other);
				helix /= n;
				sheet /= n;
				coil /= n;

				match = helix >= helixFractionMin && helix <= helixFractionMax 
						&& sheet >= sheetFractionMin && sheet <= sheetFractionMax 
						&& coil >= coilFractionMin && coil <= coilFractionMax;
			}

			if (polymer && match && ! exclusive) {
				return true;
			}
			if (polymer && ! match && exclusive) {
				return false;
			}

			if (match) {
				globalMatch = true;
			}
		}

		return globalMatch && containsPolymer;
	}
}
