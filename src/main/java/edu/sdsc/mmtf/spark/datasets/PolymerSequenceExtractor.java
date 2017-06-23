package edu.sdsc.mmtf.spark.datasets;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.mappers.StructureToPolymerSequences;
import edu.sdsc.mmtf.spark.ml.JavaRDDToDataset;

/**
 * Creates a dataset of polymer sequences using the full sequence
 * used in the experiment (i.e., the "SEQRES" record in PDB files). 
 * 
 * <p>Example: get dataset of sequences of L-proteins
 * <pre>
 * {@code
 * pdb.flatMapToPair(new StructureToPolymerChains())
 *    .filter(new ContainsLProteinChain());
 *    
 * Dataset<Row> protSeq = PolymerSequenceExtractor.getDataset(pdb);
 * protSeq.show(10);
 * }
 * </pre>
 * 
 * @author Peter Rose
 *
 */
public class PolymerSequenceExtractor {

	public static Dataset<Row> getDataset(JavaPairRDD<String, StructureDataInterface> structureRDD) {
		JavaRDD<Row> rows = structureRDD
				.flatMapToPair(new StructureToPolymerSequences())
				.map(t -> RowFactory.create(t._1, t._2));
		
		return JavaRDDToDataset.getDataset(rows, "structureChainId","sequence");
	}
}
