package edu.sdsc.mmtf.spark.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.biojava.nbio.structure.geometry.SuperPositionQCP;

/**
 * This class performs exhaustive structural alignments between two protein
 * chains. It returns zero or more alternative alignments for each pair of
 * chains based on some evaluation criteria.
 * 
 * @author Peter Rose
 * @since 0.2.0
 */
public class ExhaustiveAligner implements Serializable {
	private static final long serialVersionUID = -535570860330510219L;
	private int minLength;
	private int minCoverage;
	private double maxRmsd;
	private double minTm;

	public int getMinLength() {
		return minLength;
	}

	public void setMinLength(int minLength) {
		this.minLength = minLength;
	}

	public int getMinCoverage() {
		return minCoverage;
	}

	public void setMinCoverage(int minCoverage) {
		this.minCoverage = minCoverage;
	}

	public double getMaxRmsd() {
		return maxRmsd;
	}

	public void setMaxRmsd(double maxRmsd) {
		this.maxRmsd = maxRmsd;
	}

	public double getMinTm() {
		return minTm;
	}

	public void setMinTm(double minTm) {
		this.minTm = minTm;
	}

	// supported algorithms
	private static List<String> EXHAUSTIVE_ALGORITHMS = Arrays.asList("exhaustive");

	// "exhaustive", "dynamicProgramming"); // ... more in the future
	/**
	 * Returns true if the alignment algorithm is supported by this class.
	 * 
	 * @param alignmentAlgorithm
	 *            name of the algorithm
	 * @return true, if algorithm is supported
	 */
	public static boolean isSupportedAlgorithm(String alignmentAlgorithm) {
		return EXHAUSTIVE_ALGORITHMS.contains(alignmentAlgorithm);
	}

	/**
	 * Returns one or more structure alignments and their alignment scores.
	 * 
	 * @param alignmentAlgorithm
	 *            name of the algorithm
	 * @param key
	 *            unique identifier for protein chain pair
	 * @param points1
	 *            C-alpha positions of chain 1
	 * @param points2
	 *            C-alpha positions of chain 2
	 * @return list of alignment metrics
	 */
	public List<Row> getAlignments(String alignmentAlgorithm, String key, Point3d[] points1, Point3d[] points2) {
		List<Row> rows = new ArrayList<>();

		// TODO implement exhaustive alignments here ...

		int length = Math.min(points1.length, points2.length);

		Point3d[] x = null;
		Point3d[] y = null;

		int coverage1 = 0;
		int coverage2 = 0;

		if (points1.length != length) {
			x = Arrays.copyOfRange(points1, 0, length);
			y = points2;
			coverage1 = (int) Math.rint(100.0 * length / x.length);
			coverage2 = 100;
		} else if (points2.length != length) {
			x = points1;
			y = Arrays.copyOfRange(points2, 0, length);
			coverage1 = 100;
			coverage2 = (int) Math.rint(100.0 * length / y.length);
		}

		SuperPositionQCP qcp = new SuperPositionQCP(false);
		double rmsd = qcp.getRmsd(x, y);
		double tm = 0.0;
//		if (rmsd >= maxRmsd) {
			qcp.superposeAndTransform(x, y);
			tm = TMScore(x, y);
//		}
		System.out.println("l: " + length + " c1: " + coverage1 + " c2: " + coverage2 + " rmsd: " + rmsd + " tm: " + tm);

//		int maxCoverage = Math.max(coverage1, coverage2);

		// store solutions that satisfy minimal criteria
//		if (length >= minLength && maxCoverage >= minCoverage && tm >= minTm) {
			// create a row of alignment metrics
			Row row = RowFactory.create(key, length, coverage1, coverage2, (float)rmsd, (float)tm);
			rows.add(row);
//		}

		return rows;
	}

	/**
	 * Returns the TM-Score for two superimposed sets of coordinates Yang Zhang
	 * and Jeffrey Skolnick, PROTEINS: Structure, Function, and Bioinformatics
	 * 57:702–710 (2004)
	 * 
	 * @param x
	 *            coordinate set 1
	 * @param y
	 *            coordinate set 2
	 * @return
	 */
	public static double TMScore(Point3d[] x, Point3d[] y) {
		double d0 = 1.24 * Math.cbrt(x.length - 15.0) - 1.8;
		double d0Sq = d0 * d0;

		double sum = 0;
		for (int i = 0; i < x.length; i++) {
			sum += 1.0 / (1.0 + x[i].distanceSquared(y[i]) / d0Sq);
		}

		return sum / x.length;
	}
}
