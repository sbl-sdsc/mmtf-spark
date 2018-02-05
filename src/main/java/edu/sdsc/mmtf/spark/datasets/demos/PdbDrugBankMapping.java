package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.CustomReportService;
import edu.sdsc.mmtf.spark.datasets.DrugBankDataset;

/**
 * This class demonstrates how to map drugs from DrugBank to ligands in the PDB. 
 * Note, more specific DrugBank datasets (e.g., subset of approved drugs) can be 
 * downloaded, however, a DrugBank username and password is required
 * (see {@link DrugBankDataset}).
 * 
 * <p>
 * Reference: Wishart DS, et al., DrugBank 5.0: a major update to the DrugBank
 * database for 2018. Nucleic Acids Res. 2017 Nov 8. See
 * <a href="https://dx.doi.org/10.1093/nar/gkx1037">doi:10.1093/nar/gkx1037</a>.
 * 
 * @author Peter Rose
 * @since 0.2.0
 * 
 */
public class PdbDrugBankMapping {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName(PdbDrugBankMapping.class.getSimpleName())
                .getOrCreate();

        // download open DrugBank dataset
        Dataset<Row> drugBank = DrugBankDataset.getOpenDrugLinks();
        
        // find some tryrosine kinase inhibitors with generic name stem: "tinib"
        drugBank = drugBank.filter("Commonname LIKE '%tinib'");
        
        // get PDB ligand annotations
        Dataset<Row> ligands = CustomReportService.getDataset("ligandId","ligandMolecularWeight","ligandFormula","ligandSmiles","InChIKey");

        // join ligand dataset with DrugBank info by InChIKey
        ligands = ligands.join(drugBank, ligands.col("InChIKey").equalTo(drugBank.col("StandardInChIKey")));
       
        // show one example per drug molecule
        ligands = ligands.dropDuplicates("Commonname");
        ligands.select("structureChainId", "ligandId", "DrugBankID", "Commonname", "ligandMolecularWeight","ligandFormula", "InChIKey", "ligandSmiles")
        .sort("Commonname").show(50);

        spark.close(); 
    }
}