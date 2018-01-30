package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * This filter selects protein sequences that do not contain mutations in
 * comparison with the reference UniProt sequence. Expression tags: Some PDB
 * entries include expression tags that were added during the experiment. Select
 * "No" to filter out sequences with expression tags. Percent coverage of
 * UniProt sequence: PDB entries may contain only a portion of the referenced
 * UniProt sequence. The "Percent coverage of UniProt sequence" option defines
 * how much of a UniProt sequence needs to be contained in a PDB entry.
 * 
 * @author Peter Rose
 * @since 0.1.0
 *
 */
public class WildType implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
    private static final long serialVersionUID = -2323293283758321260L;
    private static Set<Integer> sequenceCoverage = new HashSet<>(Arrays.asList(100, 95, 90, 85, 80, 75, 70, 65, 60));
    private AdvancedQuery filter;

    /**
     * This filter selects structures that do not contain mutations in
     * comparison with the reference UniProt sequence.
     * 
     * @param includeExpressionTags
     *            if true, includes expression tags
     * @throws IOException
     */
    public WildType(boolean includeExpressionTags) throws IOException {
        String query = "<orgPdbQuery><queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>";
        if (includeExpressionTags) {
            query = query + "<includeExprTag>Y</includeExprTag>";
        } else {
            query = query + "<includeExprTag>N</includeExprTag>";
        }
        query = query + "</orgPdbQuery>";
        filter = new AdvancedQuery(query);
    }

    /**
     * This filter selects structures that do not contain mutations in
     * comparison with the reference UniProt sequence. The "Percent coverage of
     * UniProt sequence" option defines how much of a UniProt sequence needs to
     * be contained in a PDB entry
     * 
     * @param includeExpressionTags
     *            if true, includes expression tags
     * @param percentSequenceCoverage
     *            one of the following values: 100, 95, 90, 85, 80, 75, 70, 65,
     *            60
     * @throws IOException
     */
    public WildType(boolean includeExpressionTags, int percentSequenceCoverage) throws IOException {
        if (!sequenceCoverage.contains(percentSequenceCoverage)) {
            throw new IllegalArgumentException("ERROR: invalid percentSequenceCoverage: " + percentSequenceCoverage
                    + ". Choose one of the following values: " + sequenceCoverage);
        }

        String query = "<orgPdbQuery><queryType>org.pdb.query.simple.WildTypeProteinQuery</queryType>";
        if (includeExpressionTags) {
            query = query + "<includeExprTag>Y</includeExprTag>";
        } else {
            query = query + "<includeExprTag>N</includeExprTag>";
        }
        query = query + "<percentSeqAlignment>";
        query = query + percentSequenceCoverage;
        query = query + "</percentSeqAlignment>";
        query = query + "</orgPdbQuery>";
        filter = new AdvancedQuery(query);
    }

    @Override
    public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
        return filter.call(t);
    }
}
