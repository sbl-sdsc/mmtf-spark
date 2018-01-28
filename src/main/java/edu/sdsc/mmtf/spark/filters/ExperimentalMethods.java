package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns true if all the specified experimental methods match a PDB entry.
 * 
 * <p>The current list of supported experimental method types can be found
 * <a href="http://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v40.dic/Items/_exptl.method.html">here</a>
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ExperimentalMethods implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = 1373552512719676067L;
	private Set<String> experimentalMethods;
	
	// constants to be used as arguments to the Experimental Methods filter
	public static final String ELECTRON_CRYSTALLOGRAPHY = "ELECTRON CRYSTALLOGRAPHY";
	public static final String ELECTRON_MICROSCOPY = "ELECTRON MICROSCOPY";
	public static final String EPR = "EPR";
	public static final String FIBER_DIFFRACTION = "FIBER DIFFRACTION";
	public static final String FLUORESCENCE_TRANSFER = "FLUORESCENCE TRANSFER";
	public static final String INFRARED_SPECTROSCOPY = "INFRARED SPECTROSCOPY";
	public static final String NEUTRON_DIFFRACTION = "NEUTRON DIFFRACTION";
	public static final String POWDER_DIFFRACTION = "POWDER DIFFRACTION";
	public static final String SOLID_STATE_NMR = "SOLID-STATE NMR";
	public static final String SOLUTION_NMR = "SOLUTION NMR";
	public static final String SOLUTION_SCATTERING = "SOLUTION SCATTERING";
	public static final String THEORETICAL_MODEL = "THEORETICAL MODEL"; // (note, the PDB does not contain theoretical models)
	public static final String X_RAY_DIFFRACTION = "X-RAY DIFFRACTION";

	/**
	 * Constructor to define the list of experimental methods to filter by.
	 * 
	 * @param experimentalMethods comma separated list of experimental method types
	 * 
	 * {@link ExperimentalMethods#X_RAY_DIFFRACTION ExperimentalMethods.X_RAY_DIFFRACTION},
	 * {@link ExperimentalMethods#SOLUTION_NMR ExperimentalMethods.SOLUTION_NMR},
	 * {@link ExperimentalMethods#ELECTRON_MICROSCOPY ExperimentalMethods.ELECTRON_MICROSCOPY},
	 * {@link ExperimentalMethods#NEUTRON_DIFFRACTION ExperimentalMethods.NEUTRON_DIFFRACTION},
	 * {@link ExperimentalMethods#ELECTRON_CRYSTALLOGRAPHY ExperimentalMethods.ELECTRON_CRYSTALLOGRAPHY},
	 * {@link ExperimentalMethods#EPR ExperimentalMethods.EPR},
	 * {@link ExperimentalMethods#FIBER_DIFFRACTION ExperimentalMethods.FIBER_DIFFRACTION},
	 * {@link ExperimentalMethods#FLUORESCENCE_TRANSFER ExperimentalMethods.FLUORESCENCE_TRANSFER},
	 * {@link ExperimentalMethods#INFRARED_SPECTROSCOPY ExperimentalMethods.INFRARED_SPECTROSCOPY},
	 * {@link ExperimentalMethods#POWDER_DIFFRACTION ExperimentalMethods.POWDER_DIFFRACTION},
	 * {@link ExperimentalMethods#SOLID_STATE_NMR ExperimentalMethods.SOLID_STATE_NMR},
	 * {@link ExperimentalMethods#SOLUTION_SCATTERING ExperimentalMethods.SOLUTION_SCATTERING},
	 * {@link ExperimentalMethods#THEORETICAL_MODEL ExperimentalMethods.THEORETICAL_MODEL} (note, the PDB does not contain theoretical models)
	 * 
	 */
	public ExperimentalMethods(String... experimentalMethods) {
		// order the input experimental method types alphabetically
		this.experimentalMethods = new TreeSet<>(Arrays.asList(experimentalMethods));	
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		if (structure.getExperimentalMethods().length != this.experimentalMethods.size()) {
			return false;
		}

		// order experimental methods alphabetically to enable equals comparison of sets
		Set<String> methods = new TreeSet<>(Arrays.asList(structure.getExperimentalMethods()));
		methods.retainAll(experimentalMethods);
		return !methods.isEmpty();
	}
}
