/**
 * 
 */
package edu.sdsc.mmtf.spark.utils;

/**
 * Enumeration of DSSP Secondary Structure Codes.
 * 
 * <p> Reference: Kabsch W., Sander C. (1983) Dictionary of protein secondary 
 * structure: pattern recognition of hydrogen-bonded and geometrical features,
 * Biopolymers 22, 2577-2637.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public enum DsspSecondaryStructure {
	PI_HELIX('5', 0),
	BEND('S', 1),
	ALPHA_HELIX('H', 2),
	EXTENDED('E', 3),
	THREE_TEN_HELIX('G', 4),
	BRIDGE('B', 5),
	TURN('T', 6),
	COIL('C', 7);
	
	private final char oneLetterCode;
	private final int numericCode;
	
	DsspSecondaryStructure(char oneLetterCode, int numericCode) {
		this.oneLetterCode = oneLetterCode;
		this.numericCode = numericCode;
	}
	
	/**
	 * Returns the one-letter DSSP secondary structure code.
	 * @return
	 */
	public char getOneLetterCode() {
		return oneLetterCode;
	}
	
	/**
	 * Returns the DSSP secondary structure given the numeric code.
	 * @param numericCode
	 * @return
	 */
	public static DsspSecondaryStructure getDsspCode(int numericCode) {
		for (DsspSecondaryStructure d: DsspSecondaryStructure.values()) {
			if (d.numericCode == numericCode) {
				return d;
			}
		}
		return COIL;
	}
	
	/**
	 * Returns a simplified 3-state classification (Q3) of the original DSSP code.
	 * 
	 * @param numericCode
	 * @return 3-state secondary structure (ALPHA-HELIX, EXTENDED, COIL)
	 */
	public static DsspSecondaryStructure getQ3Code(int numericCode) {
		switch(numericCode) {
		case 0: return ALPHA_HELIX;
		case 1: return COIL;
		case 2: return ALPHA_HELIX;
		case 3: return EXTENDED;
		case 4: return ALPHA_HELIX;
		case 5: return EXTENDED;
		case 6: return COIL;
		case 7: return COIL;
		default: return COIL;
		}
	}		
}
