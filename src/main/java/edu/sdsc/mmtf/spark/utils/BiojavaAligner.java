package edu.sdsc.mmtf.spark.utils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.align.StructureAlignment;
import org.biojava.nbio.structure.align.StructureAlignmentFactory;
import org.biojava.nbio.structure.align.ce.CeCPMain;
import org.biojava.nbio.structure.align.ce.CeMain;
import org.biojava.nbio.structure.align.fatcat.FatCatFlexible;
import org.biojava.nbio.structure.align.fatcat.FatCatRigid;
import org.biojava.nbio.structure.align.model.AFPChain;
import org.biojava.nbio.structure.align.util.AFPChainScorer;

/**
 * This class performs structural alignments of protein chains using BioJava methods.
 * It supports both CE and FatCat algorithms.
 * 
 * @author Peter Rose
 * @since 0.2.0
 *
 */
public class BiojavaAligner implements Serializable {
	private static final long serialVersionUID = 5027430987584019776L;
	
	// supported BioJava algorithms
	private static List<String> BIOJAVA_ALGORITHMS = Arrays.asList(
			CeMain.algorithmName, CeCPMain.algorithmName,
			FatCatFlexible.algorithmName, FatCatRigid.algorithmName);
	
	// dummy names to fill data structure
	private static final String CA_NAME = "CA";
	private static final String GROUP_NAME = "GLU";

	/**
	 * Returns true if the alignment algorithm is supported by this class.
	 * @param alignmentAlgorithm name of the algorithm
	 * @return true, if algorithm is supported
	 */
	public static boolean isSupportedAlgorithm(String alignmentAlgorithm) {
		return BIOJAVA_ALGORITHMS.contains(alignmentAlgorithm);
	}
	
	/**
	 * Calculates a structural alignment and returns alignment metrics.
	 * 
	 * @param alignmentAlgorithm name of the algorithm
	 * @param key unique identifier for protein chain pair
	 * @param points1 C-alpha positions of chain 1
	 * @param points2 C-alpha positions of chain 2
	 * @return
	 */
	public static List<Row> getAlignment(String alignmentAlgorithm, String key, Point3d[] points1, Point3d[] points2) {
		// create input for BioJava alignment method
		Atom[] ca1 = getCAAtoms(points1);
		Atom[] ca2 = getCAAtoms(points2);
		
		// calculate the alignment
		AFPChain afp = null;
		try {
			StructureAlignment algorithm  = StructureAlignmentFactory.getAlgorithm(alignmentAlgorithm);
			afp = algorithm.align(ca1,ca2);
			double tmScore = AFPChainScorer.getTMScore(afp, ca1, ca2);
			afp.setTMScore(tmScore);
		} catch (StructureException e) {
			e.printStackTrace();
			return Collections.emptyList();
		} 
		
		// TODO add alignments as arrays to results
//		int[][] alignment = afp.getAfpIndex();
//		for (int i = 0; i < alignment.length; i++) {
//			System.out.println(alignment[i][0] + " - " + alignment[i][1]);
//		}

		// record the alignment metrics
		Row row = RowFactory.create(key, afp.getOptLength(), afp.getCoverage1(), 
				afp.getCoverage2(), (float) afp.getTotalRmsdOpt(), (float) afp.getTMScore());

		return Collections.singletonList(row);
	}

	/**
	 * Converts an array of points representing C-alpha position to a minimal BioJava data structure.
	 * 
	 * @param points coordinates of C-alpha atoms
	 * @return BioJava atom array
	 */
	private static Atom[] getCAAtoms(Point3d[] points) {

		Chain c = new ChainImpl();
		c.setId("A");	

		// create dummy BioJava atoms for each C-alpha coordinate
		Atom[] atoms = new Atom[points.length];

		for (int i = 0; i < points.length; i++) {
			atoms[i] = new AtomImpl();
			atoms[i].setName(CA_NAME);
			Group g = new AminoAcidImpl();
			g.setPDBName(GROUP_NAME);
			g.setResidueNumber("A", i, ' ');
			g.addAtom(atoms[i]);
			c.addGroup(g);

			atoms[i].setX(points[i].x);
			atoms[i].setY(points[i].y);
			atoms[i].setZ(points[i].z);
		}

		return atoms;
	}
}
