package edu.sdsc.mmtf.spark.filters;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.DsspSecondaryStructure;
import scala.Tuple2;

/**
 * This filter return true if the polymer sequence matches the specified regular
 * expression
 *
 * 
 * @author Peter Rose
 *
 */
public class SecondaryStructureFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	double helixFractionMin = 0;
	double helixFractionMax = 1.0;
	double sheetFractionMin = 0;
	double sheetFractionMax = 0;
	double coilFractionMin = 0;
	double coilFractionMax = 1.0;

	public SecondaryStructureFilter() {
	}

	public SecondaryStructureFilter helix(double helixFractionMin, double helixFractionMax) {
		this.helixFractionMin = helixFractionMin;
		this.helixFractionMax = helixFractionMax;
		return this;
	}

	public SecondaryStructureFilter sheet(double sheetFractionMin, double sheetFractionMax) {
		this.sheetFractionMin = sheetFractionMin;
		this.sheetFractionMax = sheetFractionMax;
		return this;
	}

	public SecondaryStructureFilter coil(double coilFractionMin, double coilFractionMax) {
		this.coilFractionMin = coilFractionMin;
		this.coilFractionMax = coilFractionMax;
		return this;
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		if (structure.getNumEntities() == 1) {
			if (structure.getSecStructList().length == 0) {
				return false;
			}

			double helix = 0;
			double sheet = 0;
			double coil = 0;

			for (int code : structure.getSecStructList()) {
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
					break;
				}
			}

			int len = structure.getSecStructList().length;

			helix /= len;
			sheet /= len;
			coil /= len;
			
//			System.out.println("helix: " + helix);
//			System.out.println("sheet: " + sheet);
//			System.out.println("coil: " + coil);

			return helix >= helixFractionMin && helix <= helixFractionMax && sheet >= sheetFractionMin
					&& sheet <= sheetFractionMax && coil >= coilFractionMin && coil <= coilFractionMax;

		}

		return false;
	}
}
