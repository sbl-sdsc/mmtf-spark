package edu.sdsc.mmtf.spark.demos;

import java.io.IOException;

import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;

public class TestBiojava {

	public static void main(String[] args) {
		 Structure s = null;
		    try {
				s = StructureIO.getStructure("1STP");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StructureException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    System.out.println(s);
	}

}
