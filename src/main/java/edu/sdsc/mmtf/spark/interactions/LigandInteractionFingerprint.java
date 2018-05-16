package edu.sdsc.mmtf.spark.interactions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.vecmath.Point3d;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.biojava.nbio.structure.symmetry.geometry.DistanceBox;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.ColumnarStructure;
import scala.Tuple2;

/**
 * Finds interactions between ligands and polymer chains and maps them onto polymer sequences.
 * 
 * @author Peter Rose
 *
 */
public class LigandInteractionFingerprint
		implements FlatMapFunction<Tuple2<String, StructureDataInterface>, Row> {
	private static final long serialVersionUID = -4696342160346858505L;
	private InteractionFilter filter;

	/**
	 * Finds interactions between ligands specified in an InteractionFilter
	 * and macromolecules.
	 * 
	 * @param filter
	 *            Specifies the conditions for calculating interactions
	 */
	public LigandInteractionFingerprint(InteractionFilter filter) {
		this.filter = filter;
	}

	@Override
	public Iterator<Row> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		String structureId = t._1;
		StructureDataInterface structure = t._2;
		return getInteractions(structureId, structure).iterator();
	}

	public List<Row> getInteractions(String structureId, StructureDataInterface structure) {
		List<Row> rows = new ArrayList<>();

		double cutoffDistanceSquared = filter.getDistanceCutoffSquared();
		ColumnarStructure arrays = new ColumnarStructure(structure, true);

		String[] chainNames = arrays.getChainNames();
		String[] groupNames = arrays.getGroupNames();
		String[] groupNumbers = arrays.getGroupNumbers();
	    String[] atomNames = arrays.getAtomNames();
		int[] entityIndices = arrays.getEntityIndices();
		String[] elements = arrays.getElements();
		boolean[] polymer = arrays.isPolymer();

		int[] sequenceMapIndices = arrays.getSequencePositions();
		float[] x = arrays.getxCoords();
		float[] y = arrays.getyCoords();
		float[] z = arrays.getzCoords();

		// create a distance box for quick lookup interactions of
		// polymer atoms of the specified elements
		DistanceBox<Integer> box = new DistanceBox<Integer>(filter.getDistanceCutoff());
		for (int i = 0; i < arrays.getNumAtoms(); i++) {
			if (polymer[i] 
			        && filter.isTargetGroup(groupNames[i]) 
			        && filter.isTargetAtomName(atomNames[i])
			        && filter.isTargetElement(elements[i])
					&& !filter.isProhibitedTargetGroup(groupNames[i])) {
				box.addPoint(new Point3d(x[i], y[i], z[i]), i);
			}
		}

		int[] groupToAtomIndices = arrays.getGroupToAtomIndices();

		for (int g = 0; g < arrays.getNumGroups(); g++) {
			// position of first and last atom +1 in group
			int start = groupToAtomIndices[g];
			int end = groupToAtomIndices[g + 1];

			// skip polymer groups
			if (polymer[start])
				continue;

			// the specified filter conditions (some groups may be excluded,
			// e.g., water)
			if (filter.isQueryGroup(groupNames[start])) {

				// create list of atoms that interact within the cutoff distance
				List<Integer> neighbors = new ArrayList<>();
				for (int a = start; a < end; a++) {
					if (filter.isQueryAtomName(atomNames[a])
					        && filter.isQueryElement(elements[a])) {
						Point3d p = new Point3d(x[a], y[a], z[a]);

						// look up neighbors that are within a cubic box
						for (int j : box.getNeighbors(p)) {
							double dx = x[j] - x[a];
							double dy = y[j] - y[a];
							double dz = z[j] - z[a];
							double dSq = dx * dx + dy * dy + dz * dz;
							if (dSq <= cutoffDistanceSquared) {
								neighbors.add(j);
							}
						}
					}
				}

				if (neighbors.isEmpty())
					continue;

				Map<String, Set<Triple<Integer, String, Integer>>> interactions2 = new TreeMap<>();
				for (int neighbor : neighbors) {
					Set<Triple<Integer, String, Integer>> list = interactions2.get(chainNames[neighbor]);
					if (list == null) {
						list = new TreeSet<Triple<Integer, String, Integer>>();
						interactions2.put(chainNames[neighbor], list);
					}

					// keep track of which group is interacting
					int seqPos = sequenceMapIndices[neighbor];
					// non-polymer groups have a negative index and are excluded
					// here
					if (seqPos >= 0) {
						list.add(new ImmutableTriple<Integer, String, Integer>(seqPos, groupNumbers[neighbor],
								entityIndices[neighbor]));
					}
				}

				for (Entry<String, Set<Triple<Integer, String, Integer>>> entry : interactions2.entrySet()) {
					Set<Triple<Integer, String, Integer>> triples = entry.getValue();
					int len = triples.size();
					Integer[] sequenceIndices = new Integer[len];
					String[] residueNames = new String[len];
					String sequence = null;

					int c = 0;
					for (Triple<Integer, String, Integer> triple : triples) {
						sequenceIndices[c] = triple.getLeft();
						residueNames[c] = triple.getMiddle();
						if (sequence == null) {
							sequence = structure.getEntitySequence(triple.getRight());
						}
						c++;
					}

					if (sequenceIndices.length > 0) {
						rows.add(RowFactory.create(structureId + "." + entry.getKey(), groupNames[start], groupNumbers[start], chainNames[start], 
						        entry.getKey(), residueNames, sequenceIndices, sequence, interactions2.size()));
					}
				}
			}
		}
		return rows;
	}	
}