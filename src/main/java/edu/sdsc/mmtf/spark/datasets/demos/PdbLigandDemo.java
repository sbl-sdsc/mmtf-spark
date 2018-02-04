package edu.sdsc.mmtf.spark.datasets.demos;

import java.io.IOException;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.sdsc.mmtf.spark.datasets.PdbjMineDataset;

/**
 * This demo shows how to query chemical components from the PDB archive. 
 * 
 * <p> This example queries the _chem_comp category. Each category
 * represents a table and fields represent database columns (see
 * <a href="https://pdbj.org/mine-rdb-docs">available tables and columns</a>).
 * 
 * <p> Example data from 1AQF.cif:
 * <pre>
 * loop_
 * _chem_comp.id 
 * _chem_comp.type 
 * _chem_comp.mon_nstd_flag 
 * _chem_comp.name 
 * _chem_comp.pdbx_synonyms 
 * _chem_comp.formula 
 * _chem_comp.formula_weight 
 * ... 
 * PEQ non-polymer         . L-PHOSPHOLACTATE ? 'C3 H7 O6 P'     170.058 
 * </pre>
 *
 * <p> Data are provided through
 * <a href="https://pdbj.org/help/mine2-sql">Mine 2 SQL</a>
 * 
 * <p> Queries can be designed with the interactive 
 * <a href="https://pdbj.org/mine/sql">PDBj Mine 2
 * query service</a>.
 * 
 * @author Peter Rose
 * @author Gert-Jan Bekker
 * @since 0.2.0
 *
 */
public class PdbLigandDemo {

   public static void main(String[] args) throws IOException {
	   SparkSession spark = SparkSession.builder().master("local[*]").appName(PdbLigandDemo.class.getSimpleName())
               .getOrCreate();

	   // find non-polymeric chemical components that contain carbon 
	   // and have a formula weight > 150 da
        String sqlQuery = "SELECT pdbid, id, formula, formula_weight, name from chem_comp "
                + " WHERE type = 'non-polymer' AND formula LIKE 'C%' AND formula_weight > 150";
        Dataset<Row> ds = PdbjMineDataset.getDataset(sqlQuery);

        System.out.println("First 10 results from query: " + sqlQuery);
        ds.show(10, false);

        System.out.println("Top 10 ligands in PDB:");
        ds.groupBy("id").count().sort(col("count").desc()).show(10);

        spark.close();
   }
}
