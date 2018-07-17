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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.utils.ColumnarStructure;
import scala.Tuple2;

/**
 * Creates a dataset of macromolecular stoichiometry (protein, DNA, RNA) for
 * the biological assemblies.
 * 
 * <p>
 * Example:
 * <pre>
 * <code>
 * pdb = ...
 * Dataset<Row> dataset = QuaternaryStructureDataset.getDataset(pdb);
 * dataset.show();
 * </code>
 * +-----------+-------------+--------------------+----------------+----------------+
 * |structureId|bioAssemblyId|proteinStoichiometry|dnaStoichiometry|rnaStoichiometry|
 * +-----------+-------------+--------------------+----------------+----------------+
 * |       1STP|            1|                  A4|            null|            null|
 * |       4HHB|            1|                A2B2|            null|            null|
 * |       5W34|            1|                  A2|              AB|            null|
 * |       3G9Y|            1|                   A|            null|               A|
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
        
        StructType schema = new StructType(new StructField[]{
                new StructField("structureId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("bioAssemblyId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("proteinStoichiometry", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dnaStoichiometry", DataTypes.StringType, true, Metadata.empty()),
                new StructField("rnaStoichiometry", DataTypes.StringType, true, Metadata.empty()),
        });
        
        SparkSession spark = SparkSession.builder().getOrCreate();
        return spark.createDataFrame(rows, schema);
    }

	private static Iterator<Row> getQuaternaryStructure(Tuple2<String, StructureDataInterface> t) throws Exception {
		List<Row> rows = new ArrayList<>();
	    String key = t._1;
		StructureDataInterface structure = t._2;
		ColumnarStructure cs = new ColumnarStructure(structure, true);
		String[] chainEntityTypes = cs.getChainEntityTypes();
		int[] chainToEntityIndex = cs.getChainToEntityIndices();
		
		for (int i = 0; i < structure.getNumBioassemblies(); i++) {
		    List<Integer> proteinIndices = new ArrayList<>();
		    List<Integer> dnaIndices = new ArrayList<>();
		    List<Integer> rnaIndices = new ArrayList<>();
		   
		    for (int j = 0; j < structure.getNumTransInBioassembly(i); j++) {
		        for (int chainIndex : structure.getChainIndexListForTransform(i, j)) {
		            int entityIndex = chainToEntityIndex[chainIndex];
		            String type = chainEntityTypes[chainIndex];
		            if (type.equals("PRO")) {
		                proteinIndices.add(entityIndex);
		            } else if (type.equals("DNA")) {
		                dnaIndices.add(entityIndex);
		            } else if (type.equals("RNA")) {
		                rnaIndices.add(entityIndex);
		            }
		        }
		    }
		    
		    String proStoich = stoichiometry(coefficients(proteinIndices));
	        String dnaStoich = stoichiometry(coefficients(dnaIndices));
	        String rnaStoich = stoichiometry(coefficients(rnaIndices));
		    rows.add(RowFactory.create(key, structure.getBioassemblyName(i), proStoich, dnaStoich, rnaStoich));
		}

		return rows.iterator();
	}

    /**
     * Returns a list of coefficients for the unique polymer entities
     * (given by entity indices) in a bioassembly.
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
        if (coefficients.isEmpty()) {
            return null;
        }
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
