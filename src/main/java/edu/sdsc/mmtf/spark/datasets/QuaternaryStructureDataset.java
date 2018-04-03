package edu.sdsc.mmtf.spark.datasets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;
import scala.Tuple2;

/**
 * Creates a dataset of macromolecular stoichiometry for
 * the biological assemblies.
 * 
 * <p>
 * Example: get dataset for protein complexes
 * 
 * <pre>
 * <code>
 * boolean exclusive = true;
 * JavaPairRDD<String, StructureDataInterface> proteinComplexes = pdb.filter(new ContainsLProteinChain(exclusive));
 * Dataset<Row> dataset = QuaternaryStructureDataset.getDataset(proteinComplexes);
 * dataset.show();
 * </code>
 * </pre>
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class QuaternaryStructureDataset {
    private static String ALPHABETH = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	/**
	 * Returns a dataset with quaternary structure info
	 * 
	 * @param structure 
	 * @return dataset quaternary structure info
	 */
	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structure) {
		JavaRDD<Row> rows = structure.flatMap(t -> getQuaternaryStructure(t));
		return JavaRDDToDataset.getDataset(rows, "structureId", "bioAssemblyId", "stoichiometry");
	}

	private static Iterator<Row> getQuaternaryStructure(Tuple2<String, StructureDataInterface> t) throws Exception {
		List<Row> rows = new ArrayList<>();
	    String key = t._1;
		StructureDataInterface structure = t._2;
		
		int[] chainToEntityIndex = getChainToEntityIndex(structure);

		for (int i = 0; i < structure.getNumBioassemblies(); i++) {
		    List<Integer> entityIndices = new ArrayList<>();
		   
		    for (int j = 0; j < structure.getNumTransInBioassembly(i); j++) {
		        for (int chainIndex : structure.getChainIndexListForTransform(i, j)) {
		            int entityIndex = chainToEntityIndex[chainIndex];
		            if (structure.getEntityType(entityIndex).equals("polymer")) {
		                entityIndices.add(chainToEntityIndex[chainIndex]);
		            }
		        }
		    }
		    
		    String stoichiometry = stoichiometry(coefficients(entityIndices));
		    rows.add(RowFactory.create(key, structure.getBioassemblyName(i), stoichiometry));
		}

		return rows.iterator();
	}

    /**
     * Returns an array that maps a chain index to an entity index.
     * @param structureDataInterface
     * @return
     */
    private static int[] getChainToEntityIndex(StructureDataInterface structure) {
        int[] entityChainIndex = new int[structure.getNumChains()];

        for (int i = 0; i < structure.getNumEntities(); i++) {
            for (int j: structure.getEntityChainIndexList(i)) {
                entityChainIndex[j] = i;
            }
        }
        return entityChainIndex;
    }
    
    /**
     * Returns a list of coefficients for the unique polymer entities
     * (give by entity indices) in a bioassembly.
     * 
     * @param entityIndices
     * @return
     */
    private static List<Integer> coefficients(List<Integer> entityIndices) {
        Map<Integer, Integer> frequencies = new TreeMap<>();
        for (int index: entityIndices) {
            Integer count = frequencies.getOrDefault(index, 0) + 1;
            frequencies.put(index, count);
        }
        List<Integer> coefficients = new ArrayList<>(frequencies.values());
        Collections.sort(coefficients, Comparator.reverseOrder());

        return coefficients;
    } 
    
    /**
     * Returns a string that encoded the polymer stoichiometry.
     * Example: 4HHB has two alpha and beta chains -> stoichiometry: A2B2
     * @param coefficients
     * @return stoichiometry string
     */
    private static String stoichiometry(List<Integer> coefficients) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < coefficients.size(); i++) {
            int coefficient = coefficients.get(i);
            sb.append(ALPHABETH.charAt(i % ALPHABETH.length()));
            if (coefficient > 1) {
                sb.append(coefficient);
            }
        }
        return sb.toString();
    }
}
