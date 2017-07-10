package edu.sdsc.mmtf.spark.incubator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.secstruc.SecStrucCalc;
import org.biojava.nbio.structure.secstruc.SecStrucState;

import scala.Tuple2;

/**
 * Maps a BioJava structure to a DSSP secondary structure string.
 * 
 * @author Peter Rose
 *
 */
public class BioJavaStructureToDssp3 implements FlatMapFunction<Iterator<Structure>, String> {
	private static final long serialVersionUID = -1234631110714432408L;
	
//	private SecStrucCalc calculator = new SecStrucCalc(); // not serializable
    
	@Override
	public Iterator<String> call(Iterator<Structure> structures) throws Exception {
		
		SecStrucCalc calculator = new SecStrucCalc();
		
		List<String> secStructures = new ArrayList<String>();
		
		while(structures.hasNext()) {
			secStructures.add(calculateSecStructure(structures.next(),
												    calculator));
		}
		
		return secStructures.iterator();
	}
		
	private String calculateSecStructure(Structure structure,
			SecStrucCalc calculator) throws Exception {
		List<SecStrucState> states = Collections.emptyList();
		
		// calculate DSSP secondary structure
		try {
			states = calculator.calculate(structure, false);
		} catch (StructureException e) {
			// this happens if there are not at least 4 consecutive residues
		}
		
		// create a DSSP string
		StringBuilder dssp = new StringBuilder();
		for (SecStrucState state: states) {
			dssp.append(state.getType().type);
		}

		return dssp.toString();
	};
}
