package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter returns entries that pass the sequence similarity search
 * criteria. Searches protein and nucleic acid sequences using the BLAST.
 * PSI-BLAST is used to find more distantly related protein sequences.
 * 
 * <p>
 * The E value, or Expect value, is a parameter that describes the number of
 * hits one can expect to see just by chance when searching a database of a
 * particular size. For example, an E value of one indicates that a result will
 * contain one sequence with similar score simply by chance. The scoring takes
 * chain length into consideration and therefore shorter sequences can have
 * identical matches with high E value.
 * 
 * <p>
 * The Low Complexity filter masks low complexity regions in a sequence to
 * filter out avoid spurious alignments.
 * 
 * <p>
 * Sequence Identity Cutoff (%) filter removes entries of low sequence
 * similarity. The cutoff value is a percentage value between 0 to 100.
 * 
 * <p>
 * Note: sequences must be at least 12 residues long. For shorter sequences try
 * the Sequence Motif Search.
 * 
 * <p>
 * References:
 * <p>
 * <ul>
 * <li>BLAST: Sequence searching using NCBI's BLAST (Basic Local Alignment
 * Search Tool) Program , Altschul, S.F., Gish, W., Miller, W., Myers, E.W. and
 * Lipman, D.J. Basic local alignment search tool. J. Mol. Biol. 215: 403-410
 * (1990)
 * <li>PSI-BLAST: Sequence searching to detect distantly related evolutionary
 * relationships using NCBI's PSI-BLAST (Position-Specific Iterated BLAST).
 * http://www.ncbi.nlm.nih.gov/Education/BLASTinfo/psi1.html
 * </ul>
 * 
 * @author Peter Rose
 * @since 0.1.0
 * @see <a href=
 *      "https://www.rcsb.org/pdb/staticHelp.do?p=help/advancedsearch/sequence.html">Sequence
 *      Search</a>
 */
public class SequenceSimilarity implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
    private static final long serialVersionUID = -8647960961587233458L;
    private AdvancedQuery filter;

    public static final String BLAST = "blast";
    public static final String PSI_BLAST = "psi-blast";

    /**
     * Filters by sequence similarity using all default parameters.
     * 
     * @throws IOException
     */
    public SequenceSimilarity(String sequence) throws IOException {
        this(sequence, BLAST, 10.0, 0, true);
    }

    /**
     * Filters by sequence similarity using the specified parameters.
     * @param sequence
     *            query sequence
     * @param searchTool
     *            SequenceSimilarity.BLAST or SequenceSimilarity,PSI_BLAST
     * @param eValueCutoff
     *            maximum e-value
     * @param sequenceIdentityCutoff
     *            minimum sequence identity cutoff
     * @param maskLowComplexity
     *            if true, mask (ignore) low complexity regions in sequences
     * @throws IOException
     */
    public SequenceSimilarity(String sequence, String searchTool, double eValueCutoff, int sequenceIdentityCutoff,
            boolean maskLowComplexity) throws IOException {

        if (sequence.length() < 12) {
            throw new IllegalArgumentException("ERROR: the query sequence must be at least 12 residues long");
        }
        String query = "<orgPdbQuery>" + "<queryType>org.pdb.query.simple.SequenceQuery</queryType>" + "<sequence>"
                + sequence + "</sequence>" + "<searchTool>" + searchTool + "</searchTool>" + "<maskLowComplexity>"
                + (maskLowComplexity ? "yes" : "no") + "</maskLowComplexity>" + "<eValueCutoff>" + eValueCutoff
                + "</eValueCutoff>" + "<sequenceIdentityCutoff>" + sequenceIdentityCutoff + "</sequenceIdentityCutoff>"
                + "</orgPdbQuery>";

        filter = new AdvancedQuery(query);
    }

    @Override
    public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
        return filter.call(t);
    }
}
