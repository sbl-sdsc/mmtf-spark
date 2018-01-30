package edu.sdsc.mmtf.spark.filters;

import java.util.regex.Pattern;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns true if the polymer sequence motif matches the specified regular expression.
 * Sequence motifs support the following one-letter codes:
 * <pre>
 * 20 standard amino acids,
 * O for Pyrrolysine,
 * U for Selenocysteine,
 * X for non-standard amino acid
 * </pre>
 * TODO list nucleic acid codes here ...
 * <p>
 * See: <a href="https://en.wikipedia.org/wiki/Sequence_motif">Sequence Motifs</a>
 * <p>
 * Examples
 * Short sequence fragment
 * <pre>
 *   NPPTP
 * </pre>
 * <p>
 * The motif search supports wildcard queries by placing a '.' at the variable residue position. A query for an SH3 domains using the consequence sequence -X-P-P-X-P (where X is a variable residue and P is Proline) can be expressed as:
 * <pre>
 *   .PP.P
 * </pre>
 * <p>
 * Ranges of variable residues are specified by the {n} notation, where n is the number of variable residues. To query a motif with seven variables between residues W and G and twenty variable residues between G and L use the following notation:
 * <pre>
 *   W.{7}G.{20}L
 * </pre>
 * <p>
 * Variable ranges are expressed by the {n,m} notation, where n is the minimum and m the maximum number of repetitions. For example the zinc finger motif that binds Zn in a DNA-binding domain can be expressed as:
 * <pre>
 *   C.{2,4}C.{12}H.{3,5}H
 * </pre>
 * <p> 
 *  The '^' operator searches for sequence motifs at the beginning of a protein sequence. The following two queries find sequences with N-terminal Histidine tags
 * <pre>
 *   ^HHHHHH or ^H{6}
 * </pre>
 * <p>
 * Square brackets specify alternative residues at a particular position. The Walker (P loop) motif that binds ATP or GTP can be expressed as:
 * <pre>
 *   [AG].{4}GK[ST]
 * </pre>
 * <p>
 * A or G are followed by 4 variable residues, then G and K, and finally S or T
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class ContainsSequenceRegex implements Function<Tuple2<String, StructureDataInterface>, Boolean> {

	private static final long serialVersionUID = -180347064409550961L;
	private Pattern pattern;
	
	public ContainsSequenceRegex(String regularExpression) {
		pattern = Pattern.compile(regularExpression);
	}

	@Override
	public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
		StructureDataInterface structure = t._2;

		for (int i = 0; i < structure.getNumEntities(); i++) {
			if (! structure.getEntitySequence(i).isEmpty()) {
				if (pattern.matcher(structure.getEntitySequence(i)).find()) {
					return true;
				}
			}
		}

		return false;
	}
}
