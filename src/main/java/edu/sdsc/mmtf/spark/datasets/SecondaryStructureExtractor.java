package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;
import edu.sdsc.mmtf.spark.utils.DsspSecondaryStructure;
import scala.Tuple2;

/**
 * Creates a dataset of 3-state secondary structure
 * (alpha, beta, coil) derived from the DSSP secondary structure
 * assignment. The dataset consists of three columns
 * with the fraction of alpha, beta, and coil within
 * a chain. The input to this class must be a single protein chain.
 * 
 * <p>Example: get dataset of secondary structure
 * <pre>
 * {@code
 * pdb.flatMapToPair(new StructureToPolymerChains())
 *    .filter(new ContainsLProteinChain());
 *    
 * Dataset<Row> secStruct = SecondaryStructureExtractor.getDataset(pdb);
 * secStruct.show(10);
 * }
 * </pre>
 * 
 * @author Peter Rose
 *
 */
public class SecondaryStructureExtractor {

	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structure) {
		JavaRDD<Row> rows = structure.map(t -> getSecStructFractions(t));
        return JavaRDDToDataset.getDataset(rows,"structureChainId","sequence","alpha","beta","coil", "dsspQ8Code", "dsspQ3Code");
	}

	private static Row getSecStructFractions(Tuple2<String, StructureDataInterface> t) {
		String key = t._1;
		StructureDataInterface structure = t._2;
		StringBuilder dsspQ8 = new StringBuilder(structure.getEntitySequence(0).length());
		StringBuilder dsspQ3 = new StringBuilder(structure.getEntitySequence(0).length());
		if (t._2.getNumChains() > 1) {
			throw new IllegalArgumentException("This method can only be applied to single polymer chains.");
		}
		float helix = 0;
		float sheet = 0;
		float coil = 0;
		int other = 0;
		int dsspIndex = 0;
		int structureIndex = 0;
		int seqIndex;
		
		for (int code: structure.getSecStructList()) {
			seqIndex = structure.getGroupSequenceIndices()[structureIndex++];
			while(dsspIndex<seqIndex)
			{
				dsspQ8.append("X");
				dsspQ3.append("X");
				dsspIndex++;
			}
			dsspQ8.append(DsspSecondaryStructure.getDsspCode(code).getOneLetterCode());
			dsspIndex++;
			switch (DsspSecondaryStructure.getQ3Code(code)) {

			case ALPHA_HELIX:
				helix++;
				dsspQ3.append("H");
				break;
			case EXTENDED:
				sheet++;
				dsspQ3.append("E");
				break;
			case COIL:
				coil++;
				dsspQ3.append("C");
				break;
			default:
				other++;
				break;
			}
		}
		while(dsspIndex < structure.getEntitySequence(0).length())
		{
			dsspQ8.append("X");
			dsspQ3.append("X");
			dsspIndex++;
		}
		
		int n = structure.getSecStructList().length - other;
		helix /= n;
		sheet /= n;
		coil /= n;	

		return RowFactory.create(key, structure.getEntitySequence(0), helix, sheet, coil, dsspQ8.toString(), dsspQ3.toString());
	}
}
