package edu.sdsc.mmtf.spark.interactions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.biojava.nbio.structure.symmetry.geometry.DistanceBox;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.ColumnarStructure;
import scala.Tuple2;

/**
 * Finds interactions between polymer chains and maps them onto polymer sequences.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class PolymerInteractionFingerprint
		implements FlatMapFunction<Tuple2<String, StructureDataInterface>, Row> {
	private static final long serialVersionUID = -4696342160346858505L;
	private InteractionFilter filter;

	/**
	 * Finds interactions between polymer chains.
	 * 
	 * @param filter
	 *            Specifies the conditions for calculating interactions
	 */
	public PolymerInteractionFingerprint(InteractionFilter filter) {
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

		// create a distance box for quickly lookup interactions of
		// polymer atoms of the specified elements
		Map<String, DistanceBox<Integer>> boxes = new LinkedHashMap<>();
		
		for (int i = 0; i < arrays.getNumAtoms(); i++) {
            if (polymer[i] 
                    && (filter.isTargetGroup(groupNames[i]) || filter.isQueryGroup(groupNames[i]))
                    && (filter.isTargetAtomName(atomNames[i]) || filter.isQueryAtomName(atomNames[i]))
                    && (filter.isTargetElement(elements[i]) || filter.isQueryElement(elements[i]))
                    && !filter.isProhibitedTargetGroup(groupNames[i])) {
			    
			    DistanceBox<Integer> box = boxes.get(chainNames[i]);
			    if (box == null) {
			        box = new DistanceBox<Integer>(filter.getDistanceCutoff());
			        boxes.put(chainNames[i], box);
			    }
			    box.addPoint(new Point3d(x[i], y[i], z[i]), i);
			    
			}
		}

		List<Entry<String, DistanceBox<Integer>>> chainBoxes = new ArrayList<>(boxes.entrySet());
		
        // loop over all pairwise polymer chain interactions
		for (int i = 0; i < chainBoxes.size()-1; i++) {
		    String chainI = chainBoxes.get(i).getKey();
		    DistanceBox<Integer> boxI = chainBoxes.get(i).getValue();

		    for (int j = i + 1; j < chainBoxes.size(); j++) {
		        String chainJ = chainBoxes.get(j).getKey();
		        DistanceBox<Integer> boxJ = chainBoxes.get(j).getValue();
		        
		        List<Integer> intersectionI = boxI.getIntersection(boxJ);
	            List<Integer> intersectionJ = boxJ.getIntersection(boxI);
		        
	            // maps to store sequence indices mapped to group numbers
	            Map<Integer,String> indicesI = new TreeMap<>();
	            Map<Integer,String> indicesJ = new TreeMap<>();

	            int entityIndexI = -1;
	            int entityIndexJ = -1;
	            
	            // loop over pairs of atom interactions and check if
	            // they satisfy the interaction filter criteria
	            for (int n = 0; n < intersectionI.size(); n++) {
	                int in = intersectionI.get(n);

	                for (int m = 0; m < intersectionJ.size(); m++) {
	                    int jm = intersectionJ.get(m);

	                    double dx = x[in] - x[jm];
	                    double dy = y[in] - y[jm];
	                    double dz = z[in] - z[jm];
	                    double dSq = dx * dx + dy * dy + dz * dz;
	                    if (dSq <= cutoffDistanceSquared) {
	                        if (filter.isTargetGroup(groupNames[in])
	                                && filter.isTargetAtomName(atomNames[in])
	                                && filter.isTargetElement(elements[in])
	                                && filter.isQueryGroup(groupNames[jm]) 
	                                && filter.isQueryAtomName(atomNames[jm]) 
	                                && filter.isQueryElement(elements[jm])) {
	                            entityIndexI = entityIndices[in];
	                            indicesI.put(sequenceMapIndices[in], groupNumbers[in]);
	                        }

	                        if (filter.isTargetGroup(groupNames[jm])
	                                && filter.isTargetAtomName(atomNames[jm])
	                                && filter.isTargetElement(elements[jm])
	                                && filter.isQueryGroup(groupNames[in]) 
	                                && filter.isQueryAtomName(atomNames[in]) 
	                                && filter.isQueryElement(elements[in])) {
	                            entityIndexJ = entityIndices[jm];
	                            indicesJ.put(sequenceMapIndices[jm], groupNumbers[jm]);
	                        }
	                    }
	                }
	            }

		        // add interactions as rows of data
		        if (indicesI.size() >= filter.getMinInteractions()) {
		            Integer[] sequenceIndiciesI = indicesI.keySet().toArray(new Integer[0]);
	                String[] groupNumbersI = indicesI.values().toArray(new String[0]);
	                
		            rows.add(RowFactory.create(structureId+"."+chainI, chainJ, chainI, groupNumbersI, sequenceIndiciesI, structure.getEntitySequence(entityIndexI)));
		        }
		        if (indicesJ.size() >= filter.getMinInteractions()) {
		            Integer[] sequenceIndiciesJ = indicesJ.keySet().toArray(new Integer[0]);
	                String[] groupNumbersJ = indicesJ.values().toArray(new String[0]);
	                
	                rows.add(RowFactory.create(structureId+"."+chainJ,  chainI, chainJ, groupNumbersJ, sequenceIndiciesJ, structure.getEntitySequence(entityIndexJ)));
		        }
		    }
		}

		return rows;
	}
}