package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.PdbjMineDataset;

/**
 * This demo shows how to query PDB annotations from the SIFTS project.
 * 
 * <p>
 * The "Structure Integration with Function, Taxonomy and Sequence"
 * (<a href="https://www.ebi.ac.uk/pdbe/docs/sifts/overview.html">SIFTS</a>) is
 * the authoritative source of up-to-date residue-level annotation of structures
 * in the PDB with data available in UniProt, IntEnz, CATH, SCOP, GO, InterPro,
 * Pfam and PubMed.
 *
 * <p>
 * Data are provided through <a href="https://pdbj.org/help/mine2-sql">Mine 2
 * SQL</a>
 * 
 * <p>
 * Queries can be designed using the interactive
 * <a href="https://pdbj.org/mine/sql">PDBj Mine 2 query service</a>.
 * 
 * @author Peter Rose
 * @author Gert-Jan Bekker
 * @since 0.2.0
 *
 */
public class SiftsDataDemo {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName(SiftsDataDemo.class.getSimpleName())
                .getOrCreate();

        // get PDB entry to PubMed Id mappings
        String pubmedQuery = "SELECT * FROM sifts.pdb_pubmed LIMIT 10";
        Dataset<Row> pubmed = PdbjMineDataset.getDataset(pubmedQuery);
        System.out.println("First 10 results for query: " + pubmedQuery);
        pubmed.show(10);

        // get PDB chain to InterPro mappings
        String interproQuery = "SELECT * FROM sifts.pdb_chain_interpro LIMIT 10";
        Dataset<Row> interpro = PdbjMineDataset.getDataset(interproQuery);
        System.out.println("First 10 results for query: " + interproQuery);
        interpro.show();

        // get PDB chain to UniProt mappings
        String uniprotQuery = "SELECT * FROM sifts.pdb_chain_uniprot LIMIT 10";
        Dataset<Row> uniprot = PdbjMineDataset.getDataset(uniprotQuery);
        System.out.println("First 10 results for query: " + uniprotQuery);
        uniprot.show();

        // get PDB chain to taxonomy mappings
        String taxonomyQuery = "SELECT * FROM sifts.pdb_chain_taxonomy LIMIT 10";
        Dataset<Row> taxonomy = PdbjMineDataset.getDataset(taxonomyQuery);
        System.out.println("First 10 results for query: " + taxonomyQuery);
        taxonomy.show();

        // get PDB chain to PFAM mappings
        String pfamQuery = "SELECT * FROM sifts.pdb_chain_pfam LIMIT 10";
        Dataset<Row> pfam = PdbjMineDataset.getDataset(pfamQuery);
        System.out.println("First 10 results for query: " + pfamQuery);
        pfam.show();

        // get PDB chain to CATH mappings
        String cathQuery = "SELECT * FROM sifts.pdb_chain_cath_uniprot LIMIT 10";
        Dataset<Row> cath = PdbjMineDataset.getDataset(cathQuery);
        System.out.println("First 10 results for query: " + cathQuery);
        cath.show();

        // get PDB chain to SCOP mappings
        String scopQuery = "SELECT * FROM sifts.pdb_chain_scop_uniprot LIMIT 10";
        Dataset<Row> scop = PdbjMineDataset.getDataset(scopQuery);
        System.out.println("First 10 results for query: " + scopQuery);
        scop.show();

        // get PDB chain to Enzyme classification (EC) mappings
        String enzymeQuery = "SELECT * FROM sifts.pdb_chain_enzyme LIMIT 10";
        Dataset<Row> enzyme = PdbjMineDataset.getDataset(enzymeQuery);
        System.out.println("First 10 results for query: " + enzymeQuery);
        enzyme.show();

        // get PDB chain to Gene Ontology term mappings
        String goQuery = "SELECT * FROM sifts.pdb_chain_go LIMIT 10";
        Dataset<Row> go = PdbjMineDataset.getDataset(goQuery);
        System.out.println("First 10 results for query: " + goQuery);
        go.show(10);

        spark.close();
    }
}
