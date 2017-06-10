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
 * a chain. The input to this class should be a single protein chain.
 * 
 * Example: get dataset of secondary structure
 * pdb.flatMapToPair(new StructureToPolymerChains())
 *    .filter(new ContainsLProteinChain());
 *    
 * Dataset<Row> secStruct = SecondaryStructureExtractor.getDataset(pdb);
 * protSeq.show(10);
 * 
 * @author Peter Rose
 *
 */
public class SecondaryStructureExtractor {

	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structure) {
		JavaRDD<Row> rows = structure.map(t -> getSecStructFractions(t));
        return JavaRDDToDataset.getDataset(rows,"structureChainId","sequence","alpha","beta","coil");
 //       return JavaRDDToDataset.getDataset(rows,"structureChainId","alpha","beta","coil");
	}

	private static Row getSecStructFractions(Tuple2<String, StructureDataInterface> t) {
		String key = t._1;
		StructureDataInterface structure = t._2;
		
		float helix = 0;
		float sheet = 0;
		float coil = 0;

		for (int code: structure.getSecStructList()) {
			switch (DsspSecondaryStructure.getQ3Code(code)) {

			case ALPHA_HELIX:
				helix++;
				break;
			case EXTENDED:
				sheet++;
				break;
			case COIL:
				coil++;
				break;
			default:
				break;
			}
		}
		
		// TODO if a full structure is the input, the fraction is
		// different from using just a chain??
		helix /= structure.getSecStructList().length;
		sheet /= structure.getSecStructList().length;
		coil /= structure.getSecStructList().length;	

		return RowFactory.create(key, structure.getEntitySequence(0), helix, sheet, coil);
//		return RowFactory.create(key, helix, sheet, coil);
	}
}
