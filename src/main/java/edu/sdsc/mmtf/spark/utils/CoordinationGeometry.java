package edu.sdsc.mmtf.spark.utils;

import java.io.Serializable;
import java.util.Arrays;

import javax.vecmath.Point3d;
import javax.vecmath.Vector3d;

/**
 * This class calculates distances, angles, and orientational order parameters 
 * for atoms coordinated to a central atom.
 * 
 * @author Peter Rose
 *
 */
public class CoordinationGeometry implements Serializable {
    private static final long serialVersionUID = 8374065053377378116L;

    private Point3d center;
    private Point3d[] neighbors;
    private double[] distances;
    private double[] angles;
    private double[][] dotProducts;

    /**
     * Constructor for CoordinationGeometry object.
     * 
     * @param center
     *            coordinates of the center atom
     * @param neighbors
     *            coordinates of the neighbor atoms
     */
    public CoordinationGeometry(Point3d center, Point3d[] neighbors) {
        this.center = center;
        this.neighbors = neighbors;
        calcDistances();
        calcAngles();
        calcDotProducts();
    }

    /**
     * Returns an array of distances from the coordination center to the
     * neighbor atoms.
     * 
     * @return distances from center
     */
    public double[] getDistances() {
        return distances;
    }

    /**
     * Returns all pairwise angles between a center and pairs of neighbor atoms.
     * 
     * @return array of pairwise angles
     */
    public double[] getAngles() {
        return angles;
    }

    /**
     * Returns a normalized trigonal orientational order parameter q3. The
     * orientational order parameter q3 measures the extent to which a molecule
     * and its three nearest neighbors adopt a trigonal arrangement. It is equal
     * to 0 for a random arrangement and equals 1 in a perfectly trigonal
     * arrangement.
     * 
     * <p>
     * Reference: Richard H. Henchman and Stuart J. Cockramb (2013), Water’s
     * Non-Tetrahedral Side, Faraday Discuss., 167, 529. <a href=
     * "https://dx.doi.org/10.1039/c3fd00080j">doi:10.1039/c3fd00080j</a>
     * 
     * @return trigonal orientational order parameter
     */
    public double q3() {
        if (neighbors.length < 3) {
            throw new IllegalArgumentException(
                    "trigonality calculation requires at least 3 neighbors, but found: " + neighbors.length);
        }

        double sum = 0.0;
        sum += Math.pow(dotProducts[0][1] + 0.5, 2);
        sum += Math.pow(dotProducts[0][2] + 0.5, 2);
        sum += Math.pow(dotProducts[1][2] + 0.5, 2);

        return 1.0 - 4.0 / 7.0 * sum;
    }

    /**
     * Returns a normalized tetrahedral orientational order parameter q4. The
     * orientational order parameter q4 measures the extent to which a central
     * atom and its four nearest neighbors adopt a tetrahedral arrangement. It
     * is equal to 0 for a random arrangement and equals 1 in a perfectly
     * tetrahedral arrangement. It can reach a minimum value of -3 for unusual
     * arrangements.
     * 
     * <P>
     * Reference: Jeffrey R. Errington & Pablo G. Debenedetti (2001)
     * Relationship between structural order and the anomalies of liquid water,
     * Nature 409, 318-321.
     * <a href="https://dx.doi.org/10.1038/35053024">doi:10.1038/35053024</a>
     * 
     * <P>
     * Reference: P.-L. Chau & A. J. Hardwick (1998) A new order parameter for
     * tetrahedral configurations, Molecular Physics, 93:3, 511-518. <a href=
     * "https://dx.doi.org/10.1080/002689798169195">doi:10.1080/002689798169195</a>
     * 
     * @return tetrahedral orientational order parameter
     */
    public double q4() {
        if (neighbors.length < 4) {
            throw new IllegalArgumentException(
                    "tetrahedrality calculation requires at least 4 neighbors, but found: " + neighbors.length);
        }

        double sum = 0.0;
        sum += Math.pow(dotProducts[0][1] + 1.0 / 3.0, 2);
        sum += Math.pow(dotProducts[0][2] + 1.0 / 3.0, 2);
        sum += Math.pow(dotProducts[0][3] + 1.0 / 3.0, 2);
        sum += Math.pow(dotProducts[1][2] + 1.0 / 3.0, 2);
        sum += Math.pow(dotProducts[1][3] + 1.0 / 3.0, 2);
        sum += Math.pow(dotProducts[2][3] + 1.0 / 3.0, 2);

        return 1.0 - 3.0 / 8.0 * sum;
    }

    /**
     * Returns a normalized trigonal bipyramidal orientational order parameter
     * q5. The first three nearest atoms to the center atom are used to define
     * the equatorial positions. The next two nearest atoms are used to specify
     * the axial positions. The orientational order parameter q5 measures the
     * extent to which the five nearest atoms adopt a trigonal bipyramidal
     * arrangement. It is equal to 0 for a random arrangement and equals 1 in a
     * perfectly trigonal bipyramidal arrangement. It can reach negative values
     * for certain arrangements.
     * 
     * <p>
     * Reference: Richard H. Henchman and Stuart J. Cockramb (2013), Water’s
     * Non-Tetrahedral Side, Faraday Discuss., 167, 529. <a href=
     * "https://dx.doi.org/10.1039/c3fd00080j">doi:10.1039/c3fd00080j</a> Note,
     * the summations in equation (3) in this paper is incorrect. This method
     * uses the corrected version (R. Henchman, personal communication).
     * 
     * @return trigonal bipyramidal orientational order parameter
     */
    public double q5() {
        if (neighbors.length < 5) {
            throw new IllegalArgumentException(
                    "trigonal bipyramidality calculation requires at least 5 neighbors, but found: "
                            + neighbors.length);
        }

        // 3 equatorial-equatorial angles (120 deg: cos(120) = -1/2)
        double sum1 = 0.0;
        sum1 += Math.pow(dotProducts[0][1] + 1.0 / 2.0, 2);
        sum1 += Math.pow(dotProducts[0][2] + 1.0 / 2.0, 2);
        sum1 += Math.pow(dotProducts[1][2] + 1.0 / 2.0, 2);

        // 6 equatorial-axial angles (90 deg: cos(90) = 0)
        double sum2 = 0.0;
        sum2 += Math.pow(dotProducts[0][3], 2);
        sum2 += Math.pow(dotProducts[0][4], 2);
        sum2 += Math.pow(dotProducts[1][3], 2);
        sum2 += Math.pow(dotProducts[1][4], 2);
        sum2 += Math.pow(dotProducts[2][3], 2);
        sum2 += Math.pow(dotProducts[2][4], 2);

        // 1 axial-axial angle (180 deg: cos(180) = -1)
        double sum3 = Math.pow(dotProducts[3][4] + 1, 2);

        return 1.0 - 6.0 / 35.0 * sum1 - 3.0 / 10.0 * sum2 - 3.0 / 40.0 * sum3;
    }

    /**
     * Returns a normalized octahedra orientational order parameter q6. The
     * orientational order parameter q6 measures the extent to which a central
     * atom and its six nearest neighbors adopt an octahedral arrangement. It is
     * equal to 0 for a random arrangement and equals 1 in a perfectly
     * octahedralhedral arrangement. It can reach negative values for certain
     * arrangements.
     * 
     * <p>
     * Reference: Richard H. Henchman and Stuart J. Cockramb (2013), Water’s
     * Non-Tetrahedral Side, Faraday Discuss., 167, 529. <a href=
     * "https://dx.doi.org/10.1039/c3fd00080j">doi:10.1039/c3fd00080j</a> The
     * same method as described in this paper was used to derive the q6
     * parameter (R. Henchman, personal communication).
     * 
     * @return octahedral orientational order parameter
     */
    public double q6() {
        if (neighbors.length < 6) {
            throw new IllegalArgumentException(
                    "octrahedrality calculation requires at least 6 neighbors, but found: " + neighbors.length);
        }

        double sum = 0.0;
        // 4 angles between positions in the "equatorial" plane
        sum += Math.pow(dotProducts[0][1], 2);
        sum += Math.pow(dotProducts[1][2], 2);
        sum += Math.pow(dotProducts[2][3], 2);
        sum += Math.pow(dotProducts[3][0], 2);

        // 4 angles between "axial" position 1 and "equatorial" plane
        sum += Math.pow(dotProducts[0][4], 2);
        sum += Math.pow(dotProducts[1][4], 2);
        sum += Math.pow(dotProducts[2][4], 2);
        sum += Math.pow(dotProducts[3][4], 2);

        // 4 angles between "axial" position 2 and "equatorial" plane
        sum += Math.pow(dotProducts[0][5], 2);
        sum += Math.pow(dotProducts[1][5], 2);
        sum += Math.pow(dotProducts[2][5], 2);
        sum += Math.pow(dotProducts[3][5], 2);

        return 1.0 - 1.0 / 4.0 * sum;
    }

    private void calcDistances() {
        distances = new double[neighbors.length];
        for (int i = 0; i < neighbors.length; i++) {
            distances[i] = center.distance(neighbors[i]);
        }
    }

    private void calcDotProducts() {
        int[] index = getIndexByDistance(distances);
        Vector3d[] vectors = new Vector3d[neighbors.length];

        for (int i = 0; i < neighbors.length; i++) {
            vectors[i] = new Vector3d(center);
            vectors[i].sub(neighbors[index[i]]);
            vectors[i].normalize();
        }

        dotProducts = new double[neighbors.length][neighbors.length];
        for (int i = 0; i < vectors.length - 1; i++) {
            for (int j = i + 1; j < vectors.length; j++) {
                dotProducts[i][j] = vectors[i].dot(vectors[j]);
            }
        }
    }

    private void calcAngles() {
        Vector3d[] vectors = getVectors();

        angles = new double[neighbors.length * (neighbors.length - 1) / 2];

        for (int i = 0, n = 0; i < vectors.length - 1; i++) {
            for (int j = i + 1; j < vectors.length; j++, n++) {
                angles[n] = vectors[i].angle(vectors[j]);
            }
        }
    }

    private Vector3d[] getVectors() {
        Vector3d[] vectors = new Vector3d[neighbors.length];

        for (int i = 0; i < neighbors.length; i++) {
            vectors[i] = new Vector3d(center);
            vectors[i].sub(neighbors[i]);
        }

        return vectors;
    }

    private static int[] getIndexByDistance(double[] values) {
        int index[] = new int[values.length];
        Item[] elements = new Item[values.length];
        for (int i = 0; i < values.length; i++) {
            elements[i] = new Item(i, values[i]);
        }
        Arrays.sort(elements);
        for (int i = 0; i < values.length; i++) {
            index[i] = elements[i].index;
        }
        return index;
    }

    private static class Item implements Comparable<Item> {
        private int index;
        private double value;

        Item(int index, double value) {
            this.index = index;
            this.value = value;
        }

        public int compareTo(Item e) {
            return Double.compare(this.value, e.value);
        }
    }
}
