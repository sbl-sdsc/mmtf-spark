package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.DrugBankDataset;

/**
 * This class demonstrates how to access the open DrugBank dataset. This
 * datasets contains identifiers and names for integration with other data
 * resources.
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
public class DrugBankDemo {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName(DrugBankDemo.class.getSimpleName())
                .getOrCreate();

        // download open DrugBank dataset
        Dataset<Row> openDrugLinks = DrugBankDataset.getOpenDrugLinks();

        // find all drugs with an InChIKey
        openDrugLinks = openDrugLinks.filter("StandardInChIKey IS NOT NULL");

        // show some sample data
        openDrugLinks.select("DrugBankID", "Commonname", "CAS", "StandardInChIKey").show();

        // The DrugBank password protected datasets contain more information.
        // You need to create a DrugBank account and supply username/password
        // to access these datasets.

        // Download DrugBank dataset for approved drugs
        // String username = args[0];
        // String password = args[1];
        // Dataset<Row> drugLinks =
        // DrugBankDataset.getDrugLinks(DrugGroup.APPROVED, username, password);
        // drugLinks.show();

        spark.close(); 
    }
}