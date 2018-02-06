package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Returns rows of continuous segments of protein sequence with the specified
 * DSSP secondary structure code (E, H, C) of a minimum length.
 * 
 * @author Yue Yu
 *
 */
public class StructureToSecondaryStructureElements implements FlatMapFunction<Row, Row> {

	private static final long serialVersionUID = -4554899194610295337L;
	private String label;
	private int minLength;

	/**
	 * Returns a rows of continuous segments of protein sequence with the
	 * specified DSSP secondary structure code (E, H, C) of a minimum length.
	 * 
	 * @param label
	 *            DSSP label of the secondary structure element (E, H, C)
	 * @param minLength
	 *            minimum length of the secondary structure element
	 */
	public StructureToSecondaryStructureElements(String label, int minLength) {
		this.label = label;
		this.minLength = minLength;
	}

	@Override
	public Iterator<Row> call(Row t) throws Exception {
		// get information from the input Row
		String sequence = t.getString(1);
		String dsspQ3 = t.getString(6);

		int currLength = 0;
		String currSequence = "";
		int j;
		List<Row> sequences = new ArrayList<>();

		for (int i = 0; i < sequence.length(); i++) {
			currLength = 0;
			currSequence = "";
			for (j = i; j < sequence.length(); j++) {
				if (dsspQ3.substring(j, j + 1).equals(label)) {
					currLength++;
					currSequence = currSequence.concat(sequence.substring(j, j + 1));
				} else
					break;
			}
			i += currLength;
			if (currLength >= minLength) {
				sequences.add(RowFactory.create(currSequence, label));
			}
		}
		return sequences.iterator();
	}
}