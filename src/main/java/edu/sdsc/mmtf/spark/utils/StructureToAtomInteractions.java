package edu.sdsc.mmtf.spark.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.symmetry.geometry.DistanceBox;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.interactions.InteractionFilter;
import scala.Tuple2;

/**
 * Finds interactions that match the criteria specified by the InteractionFilter.
 * 
 * @author Peter Rose
 *
 */
public class StructureToAtomInteractions
		implements FlatMapFunction<Tuple2<String, StructureDataInterface>, Row> {
	private static final long serialVersionUID = -3348372120358649240L;
	private InteractionFilter filter;
	private boolean pairwise = false;

	/**
	 * This constructor specifies to calculate the interaction of 
	 * a specific group and a specific atom,.e.g. the atom "O" in
	 * the group "HOH".
	 * @param filter Specifies the conditions for calculating interactions
	 * @param pairwise If true, results as one row per pair interaction, otherwise
	 * the interactions of one atom with all other atoms are returned as a single row.
	 */
	public StructureToAtomInteractions(Broadcast<InteractionFilter> bfilter, boolean pairwise) {
		this.filter = bfilter.getValue();
		this.pairwise = pairwise;
	}
	
	@Override
	public Iterator<Row> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		List<Row> interactions = new ArrayList<>();
		
		String structureId = t._1;
		
		// convert structure to an array-based format for efficient processing
		ColumnarStructureX arrays = new ColumnarStructureX(t._2, true);

		// create a the list of query atoms for which interactions should be calculated

		List<Integer> queryAtomIndices = getQueryAtomIndices(arrays);

		// no query atoms found, return an empty iterator
		if (queryAtomIndices.isEmpty()) {
			return interactions.iterator();
		}

		// Add atom (indices) on grid for rapid indexing of atom
		// neighbors on a grid based on a cutoff distance
		DistanceBox<Integer> box = getDistanceBox(arrays);

		for (int queryAtomIndex : queryAtomIndices) {
			// find interactions of query atom specified by atom index
			AtomInteraction interaction = getInteractions(arrays, queryAtomIndex, box);
			interaction.setStructureId(structureId);

			// only add interactions that are within the given limits of interactions
			if (interaction.getNumInteractions() >= filter.getMinInteractions()
					&& interaction.getNumInteractions() <= filter.getMaxInteractions()) {
				
				// return interactions as either pairs or all interaction of one atom as a row
				if (pairwise) {
					interactions.addAll(interaction.getPairInteractionsAsRows());
				} else {
				    interactions.add(interaction.getMultipleInteractionsAsRow(filter.getMaxInteractions()));
				}
			}
		}

		return interactions.iterator();
	}

	private DistanceBox<Integer> getDistanceBox(ColumnarStructure arrays) {
		DistanceBox<Integer> box = new DistanceBox<>(filter.getDistanceCutoff());

		float[] x = arrays.getxCoords();
		float[] y = arrays.getyCoords();
		float[] z = arrays.getzCoords();
		String[] elements = arrays.getElements();
		String[] groupNames = arrays.getGroupNames();

		for (int i = 0; i < arrays.getNumAtoms(); i++) {
			if (filter.isTargetGroup(groupNames[i]) && filter.isTargetElement(elements[i])) {
				box.addPoint(new Point3d(x[i], y[i], z[i]), i);
			}
		}

		return box;
	}

	private AtomInteraction getInteractions(ColumnarStructureX arrays, int queryAtomIndex, DistanceBox<Integer> box) {
		AtomInteraction interaction = new AtomInteraction();

		float[] x = arrays.getxCoords();
		float[] y = arrays.getyCoords();
		float[] z = arrays.getzCoords();

		float qx = x[queryAtomIndex];
		float qy = y[queryAtomIndex];
		float qz = z[queryAtomIndex];

		int[] atomToGroupIndices = arrays.getAtomToGroupIndices();
		float[] occupancies = arrays.getOccupancies();
		float[] normalizedbFactors = arrays.getNormalizedbFactors();
		String[] groupNames = arrays.getGroupNames();

		// record query atom info
		InteractionCenter queryCenter = new InteractionCenter(arrays, queryAtomIndex);
		interaction.setCenter(queryCenter);

		double cutoffDistanceSq = filter.getDistanceCutoffSquared();
		// Retrieve atom indices of atoms that lay within grid cubes
		// that are within cutoff distance of the query atom
		List<Integer> neighborIndices = box.getNeighbors(new Point3d(qx, qy, qz));

		// determine and record interactions with neighbor atoms
		for (int neighborIndex : neighborIndices) {

			// exclude self interactions with a group
			if (atomToGroupIndices[neighborIndex] == atomToGroupIndices[queryAtomIndex])
				continue;

			// check if interaction is within distance cutoff
			double dx = qx - x[neighborIndex];
			double dy = qy - y[neighborIndex];
			double dz = qz - z[neighborIndex];
			double distSq = dx * dx + dy * dy + dz * dz;

			if (distSq <= cutoffDistanceSq) {
				// exclude interactions with undesired groups and
				// atoms with partial occupancy (< 1.0)
				if (filter.isProhibitedTargetGroup(groupNames[neighborIndex])
						|| normalizedbFactors[neighborIndex] > filter.getNormalizedbFactorCutoff()
	                    || occupancies[neighborIndex] < 1.0f ) {
					// return an empty atom interaction
					return new AtomInteraction();
				}
				
				// add interacting atom info
				InteractionCenter neighbor = new InteractionCenter(arrays, neighborIndex);
				interaction.addNeighbor(neighbor);
				
				// terminate early if the number of interactions exceeds limit
				if (interaction.getNumInteractions() > filter.getMaxInteractions()) {
					return interaction;
				}
			}
		}

		return interaction;
	}

	/**
	 * Returns a list of indices to query atoms in the structure
	 * 
	 * @param ColumnarStructureX
	 * @return
	 */
	private List<Integer> getQueryAtomIndices(ColumnarStructureX arrays) {
		// get required data
		String[] groupNames = arrays.getGroupNames();
		String[] elements = arrays.getElements();
		int[] groupStartIndices = arrays.getGroupToAtomIndices();
		float[] occupancies = arrays.getOccupancies();
		float[] normalizedbFactors = arrays.getNormalizedbFactors();
		
		// find atoms that match the query criteria and 
		// exclude atoms with partial occupancy
		List<Integer> indices = new ArrayList<>();
		for (int i = 0; i < arrays.getNumGroups(); i++) {
			int start = groupStartIndices[i];
			int end = groupStartIndices[i+1];
			
			if (filter.isQueryGroup(groupNames[start])) {
				for (int j = start; j < end; j++) {
					if (filter.isQueryElement(elements[j])
					        && normalizedbFactors[j] < filter.getNormalizedbFactorCutoff()
							&& !(occupancies[j] < 1.0f)) {
						indices.add(j);
					}
				}
			}
		}
		return indices;
	}
}