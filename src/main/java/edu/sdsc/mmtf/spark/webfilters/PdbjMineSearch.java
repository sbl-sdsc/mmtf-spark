package edu.sdsc.mmtf.spark.webfilters;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.datasets.PdbjMineDataset;
import scala.Tuple2;

/**
 * This filter runs an PDBj Mine 2 Search web service using an SQL query.
 * 
 * <p> Each category represents a table, and fields represent database columns (see
 * <a href="https://pdbj.org/mine-rdb-docs">available tables and columns</a>.
 * 
 * <p> Data are provided through
 * <a href="https://pdbj.org/help/mine2-sql">Mine 2 SQL</a>
 * 
 * <p> Queries can be designed with the interactive 
 * <a href="https://pdbj.org/mine/sql">PDBj Mine 2
 * query service</a>.
 * 
 * @author Gert-Jan Bekker
 * @since 0.1.0
 *
 */
public class PdbjMineSearch implements Function<Tuple2<String, StructureDataInterface>, Boolean> {
    private static final long serialVersionUID = -4794067375376198086L;
    private static final String SERVICELOCATION = "https://pdbj.org/rest/mine2_sql";
    private Set<String> pdbIds;
    private boolean chainLevel = false;

    /**
     * Fetches data using the PDBj Mine 2 SQL service
     * 
     * @param sqlQuery
     *            query in SQL format
     * @throws IOException
     */
	public PdbjMineSearch(String sqlQuery) throws IOException {

        String encodedSQL = URLEncoder.encode(sqlQuery, "UTF-8");

        URL u = new URL(SERVICELOCATION + "?format=csv&q=" + encodedSQL);
        InputStream in = u.openStream();

        // save as a temporary CSV file
        Path tempFile = Files.createTempFile(null, ".csv");
        Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
        in.close();

        Dataset<Row> ds = PdbjMineDataset.getDataset(sqlQuery);
        
        // extract structureIds and structureChainIds to be used for filtering
        pdbIds = new HashSet<>();
        
        List<String> columns = Arrays.asList(ds.columns());
        if (columns.contains("structureId")) {
            chainLevel = false;
            pdbIds.addAll(new HashSet<String>(ds.select("structureId").as(Encoders.STRING()).collectAsList()));
        }
            
        if (columns.contains("structureChainId")) {
             chainLevel = true;
        	 List<String> ids = ds.select("structureChainId").as(Encoders.STRING()).collectAsList();
        	 for (String id: ids) {
                pdbIds.add(id); // structureChainId (e.g., 4HHB.A)
                pdbIds.add(id.substring(0,4)); // structureId (e.g., 4HHB
         } 
        }
    }

    @Override
    public Boolean call(Tuple2<String, StructureDataInterface> t) throws Exception {
        boolean match = pdbIds.contains(t._1);

        // if results are PDB IDs, but the keys contains chain names,
        // then truncate the chain name before matching (e.g., 4HHB.A -> 4HHB)
        if (!chainLevel && !match && t._1.length() > 4)
            match = pdbIds.contains(t._1.substring(0, 4));

        return match;
    }
}
