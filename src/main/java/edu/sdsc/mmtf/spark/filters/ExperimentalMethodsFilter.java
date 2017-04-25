package edu.sdsc.mmtf.spark.filters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter return true if all the specified methods match.
 * @author Peter Rose
 *
 */
public class ExperimentalMethodsFilter implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
	private static final long serialVersionUID = -4794067375376198086L;
	private String experimentalMethod;
	private Set<String> experimentalMethods;
	
	public ExperimentalMethodsFilter(String... experimentalMethods) {
		
		if (experimentalMethods.length == 1) {
			this.experimentalMethod = experimentalMethods[0];
		} else {
			this.experimentalMethods = new TreeSet<>(Arrays.asList(experimentalMethods));	
		}
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;
	
		if (structure.getExperimentalMethods().length == 1) {
			return structure.getExperimentalMethods()[0].equals(experimentalMethod);
		} else {
			Set<String> methods = new TreeSet<>(Arrays.asList(structure.getExperimentalMethods()));
			return methods.equals(experimentalMethods);
		}

	}
}
