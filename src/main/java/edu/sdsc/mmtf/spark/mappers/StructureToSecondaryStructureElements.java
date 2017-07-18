package edu.sdsc.mmtf.spark.mappers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * Maps chain sequences to its sequence segments. 
 * 
 * TODO
 * @author Yue Yu
 */
public class StructureToSecondaryStructureElements implements FlatMapFunction<Row, Row> {

	private static final long serialVersionUID = -4554899194610295337L;
	private String label;
	private int length;
	/*
	 * Constructor sets the segment length. The length must
	 * be an odd number
	 */
	public StructureToSecondaryStructureElements(String label) {
		this.label = label;
		this.length = 4;
	}
	public StructureToSecondaryStructureElements(String label, int length) {
		this.label = label;
		this.length = length;
	}
	
	@Override
	public Iterator<Row> call(Row t) throws Exception {
		//get information from the input Row
		String sequence = t.getString(1);
		String dsspQ3 = t.getString(6);

		int currLength = 0;
		String currSequence = "";
		int j;
		List<Row> sequences = new ArrayList<>();
		
		for (int i = 0; i < sequence.length(); i++)
		{
			currLength = 0;
			currSequence = "";
			for(j = i; j < sequence.length(); j++)
			{
				if(dsspQ3.substring(j, j+1).equals(label))
				{
					currLength++;
					currSequence = currSequence.concat(sequence.substring(j, j+1));
				}
				else break;
			}
			i += currLength;
			if(currLength >= length)
			{
				sequences.add( RowFactory.create(currSequence, label) );
			}
		}
		return sequences.iterator();
	}
}