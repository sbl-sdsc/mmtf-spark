package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns true if all the specified experimental methods match a PDB entry.
 * Currently, the following experimental method types are supported:
 * 
 * "ELECTRON CRYSTALLOGRAPHY"
 * "ELECTRON MICROSCOPY"
 * "EPR"
 * "FIBER DIFFRACTION"
 * "FLUORESCENCE TRANSFER"
 * "INFRARED SPECTROSCOPY"
 * "NEUTRON DIFFRACTION"
 * "POWDER DIFFRACTION"
 * "SOLID-STATE NMR"
 * "SOLUTION NMR"
 * "SOLUTION SCATTERING"
 * "THEORETICAL MODEL" (note, the PDB does not contain theoretical models)
 * "X-RAY DIFFRACTION"
 * 
 * The current list of supported experimental method types can be found
 * <a href="http://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v40.dic/Items/_exptl.method.html">here</a>
 * 
 * @author Peter Rose
 *
 */
public class ExperimentalMethodsFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = 1373552512719676067L;
	private String experimentalMethod;
	private Set<String> experimentalMethods;
	
	/**
	 * Constructor to define the list of experimental methods to filter by.
	 * @param experimentalMethods comma separate list of experimental method types
	 */
	public ExperimentalMethodsFilter(String... experimentalMethods) {
		
		// in most cases, we only search for one method. 
		// For performance reasons this case is handled separately.
//		if (experimentalMethods.length == 1) {
//			this.experimentalMethod = experimentalMethods[0];
//		} else {
//			// order the input experimental method types alphabetically
			this.experimentalMethods = new TreeSet<>(Arrays.asList(experimentalMethods));	
//		}
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		if (structure.getExperimentalMethods().length != this.experimentalMethods.size()) {
			return false;
		}
		
//		if (structure.getExperimentalMethods().length == 1) {
//			return structure.getExperimentalMethods()[0].equals(experimentalMethod);
//		} else {
//			// if there is more than one experimental method type, order them alphabetically
			Set<String> methods = new TreeSet<>(Arrays.asList(structure.getExperimentalMethods()));
			return methods.equals(experimentalMethods);
//		}
	}
}
