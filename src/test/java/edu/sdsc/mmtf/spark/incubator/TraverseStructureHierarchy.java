package edu.sdsc.mmtf.spark.incubator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import edu.sdsc.mmtf.spark.io.demos.ReadMmtfReduced;
import edu.sdsc.mmtf.spark.utils.DsspSecondaryStructure;
import scala.Tuple3;

/**
 * This class demonstrates how to access different categories of data from
 * the StructureDataInterface and how to traverse the structure hierarchy.
 * 
 * @author Peter Rose
 */
public class TraverseStructureHierarchy {

	public static void main(String args[]) {
		String path = System.getProperty("MMTF_FULL");
	    if (path == null) {
	    	    System.err.println("Environment variable for Hadoop sequence file has not been set");
	        System.exit(-1);
	    }
		// instantiate Spark. Each Spark application needs these two lines of code.
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReadMmtfReduced.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		//	    List<String> pdbIds = Arrays.asList("5UTV"); // multiple models
		//	    List<String> pdbIds = Arrays.asList("1BZ1"); // multiple protein chains
		//      List<String> pdbIds = Arrays.asList("1STP"); // single protein chain
		List<String> pdbIds = Arrays.asList("1HV4"); // structure with 2 bioassemblies
		pdbIds = Arrays.asList("5LXK","1STP","4HHB");
		//	    List<String> pdbIds = Arrays.asList("2NBK"); // single protein chain
		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache();
//		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, pdbIds, sc).cache(); 

		pdb.map(t -> new Tuple3<String,String,String>(t._1,t._2.getMmtfProducer(), t._2.getReleaseDate())).foreach(t -> System.out.println(t));

		//		pdb.foreach(t -> TraverseStructureHierarchy.demo(t._2));      
	}

	/*
	 * Demonstrates how to access information from Structure DataInterface
	 * and how to traverse the data hierarchy.
	 * 
	 * @param structure structure to be traversed
	 */
	public static void demo(StructureDataInterface structure) {
		printMmtfInfo(structure);
		printMetadata(structure);
		printCrystallographicData(structure);
		traverse(structure);
		printChainInfo(structure);
		printChainGroupInfo(structure);
		printChainEntityGroupAtomInfo(structure);
		printBioAssemblyData(structure);
		// other data not printed here include:
		// intragroup bonds
		// intergroup bonds
	}

	/**
	 * Prints MMTF version info.
	 * 
	 * @param structure structure to be traversed
	 */
	private static void printMmtfInfo(StructureDataInterface structure) {
		System.out.println("*** MMMTF INFO ***");
		System.out.println("MmtfProducer    : " + structure.getMmtfProducer());
		System.out.println("MmtfVersion     : " + structure.getMmtfVersion());
		System.out.println();
	}

	/**
	 * Prints metadata.
	 * 
	 * @param structure structure to be traversed
	 */
	private static void printMetadata(StructureDataInterface structure) {
		System.out.println("*** METADATA ***");
		System.out.println("StructureId           : " + structure.getStructureId());
		System.out.println("Title                 : " + structure.getTitle());
		System.out.println("Deposition date       : " + structure.getDepositionDate());
		System.out.println("Release date          : " + structure.getReleaseDate()); // note, this is currently the last update date
		System.out.println("Experimental method(s): " + Arrays.toString(structure.getExperimentalMethods()));
		System.out.println("Resolution            : " + structure.getResolution());
		System.out.println("Rfree                 : " + structure.getRfree());
		System.out.println("Rwork                 : " + structure.getRwork());
		System.out.println();
	}

	/**
	 * Prints crystallographic information.
	 * 
	 * @param structure structure to be traversed
	 */
	private static void printCrystallographicData(StructureDataInterface structure) {
		System.out.println("*** CRYSTALLOGRAPHIC DATA ***");
		System.out.println("Space group           : " + structure.getSpaceGroup());
		System.out.println("Unit cell dimensions  : " + Arrays.toString(structure.getUnitCell()));	
		System.out.println();
	}

	public static void printBioAssemblyData(StructureDataInterface structure) {
		System.out.println("*** BIOASSEMBLY DATA ***");
		System.out.println("Number bioassemblies: " + structure.getNumBioassemblies());
		
		for (int i = 0; i < structure.getNumBioassemblies(); i++) {
			System.out.println("bioassembly: " + structure.getBioassemblyName(i));
			int numTransformations = structure.getNumTransInBioassembly(i);
			System.out.println("  Number transformations: " + numTransformations);
			for (int j = 0; j < numTransformations; j++) {
				System.out.println("    transformation: " + j);
				System.out.println("    chains:         " + Arrays.toString(structure.getChainIndexListForTransform(i, j)));
				System.out.println("    rotTransMatrix: " + Arrays.toString(structure.getMatrixForTransform(i, j)));
			}
		}
	}

	/**
	 * Traverses the basic data hierarchy.
	 * 
	 * @param structure structure to be traversed
	 */
	public static void traverse(StructureDataInterface structure) {

		System.out.println("*** STRUCTURE DATA ***");
		System.out.println("Number of models  : " + structure.getNumModels());
		System.out.println("Number of entities: " + structure.getNumEntities());
		System.out.println("Number of chains  : " + structure.getNumChains());
		System.out.println("Number of groups  : " + structure.getNumGroups());
		System.out.println("Number of atoms   : " + structure.getNumAtoms());

		// Global indices that point into the flat (columnar) data structure
		// e.g., all x-coordinates are in a single float[] and are indexed by
		// atomIndex.
		int chainIndex = 0;
		int groupIndex = 0;
		int atomIndex = 0;

		// Loop over models
		for (int i = 0; i < structure.getNumModels(); i++) {

			// Loop over chains in a model
			for (int j = 0; j < structure.getChainsPerModel()[i]; j++) {

				// Loop over groups in a chain
				for (int k = 0; k < structure.getGroupsPerChain()[chainIndex]; k++) {

					// Unique group (residues) are stored only once in a dictionary. 
					// We need to get the group type to retrieve group information
					int groupType = structure.getGroupTypeIndices()[groupIndex];

					// Loop over atoms in a group retrieved from the dictionary
					for (int m = 0; m < structure.getNumAtomsInGroup(groupType); m++) {
						atomIndex++; // update global atom index
					}
					groupIndex++; // update global group index
				}
				chainIndex++; // update global chain index
			}
		}

		System.out.println("chainIndex: " + chainIndex);
		System.out.println("groupIndex: " + groupIndex);
		System.out.println("atomIndex : " + atomIndex);
		System.out.println();
	}

	/**
	 * Traverses the basic hierarchy of the data structure and prints
	 * chain information
	 * 
	 * @param structure structure to be traversed
	 */
	public static void printChainInfo(StructureDataInterface structure) {

		System.out.println("*** CHAIN DATA ***");
		System.out.println("Number of chains: " + structure.getNumChains());

		int chainIndex = 0;

		// Loop over models
		for (int i = 0; i < structure.getNumModels(); i++) {
			System.out.println("model: " + (i+1)); // models are 1-based

			// Loop over chains in a model
			for (int j = 0; j < structure.getChainsPerModel()[i]; j++) {

				// Print chain info
				String chainName = structure.getChainNames()[chainIndex]; // chain name used in pdb files
				String chainId = structure.getChainIds()[chainIndex]; // called asym_id in mmCIF
				int groups = structure.getGroupsPerChain()[chainIndex];
				System.out.println("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + groups);

				chainIndex++;
			}
		}
		System.out.println();
	}

	/**
	 * Traverses the basic hierarchy of the data structure and prints
	 * chain and group information.
	 * 
	 * @param structure structure to be traversed
	 */
	public static void printChainGroupInfo(StructureDataInterface structure) {
		System.out.println("*** CHAIN AND GROUP DATA ***");

		// Global indices that point into the flat (columnar) data structure
		int chainIndex = 0;
		int groupIndex = 0;

		// Loop over models
		for (int i = 0; i < structure.getNumModels(); i++) {
			System.out.println("model: " + (i+1));

			// Loop over chains in a model
			for (int j = 0; j < structure.getChainsPerModel()[i]; j++) {

				// Print chain info
				String chainName = structure.getChainNames()[chainIndex]; // this is the chain name used in pdb files
				String chainId = structure.getChainIds()[chainIndex]; // this is also called asym_id in mmCIF			
				int groups = structure.getGroupsPerChain()[chainIndex];
				System.out.println("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + groups);

				// Loop over groups in a chain
				for (int k = 0; k < structure.getGroupsPerChain()[chainIndex]; k++) {

					// get group data
					int groupId = structure.getGroupIds()[groupIndex]; // aka residue number
					char insertionCode = structure.getInsCodes()[groupIndex];
					int secStruct = structure.getSecStructList()[groupIndex];
					int seqIndex = structure.getGroupSequenceIndices()[groupIndex];

					// Unique groups (residues) are stored only once in a dictionary. 
					// We need to get the group type to retrieve group information
					int groupType = structure.getGroupTypeIndices()[groupIndex];	

					// retrieve group info from dictionary
					String groupName = structure.getGroupName(groupType);
					String chemCompType = structure.getGroupChemCompType(groupType);
					char oneLetterCode = structure.getGroupSingleLetterCode(groupType);
					int numAtoms = structure.getNumAtomsInGroup(groupType);
					int numBonds = structure.getGroupBondOrders(groupType).length;

					System.out.println("   groupName      : " + groupName);
					System.out.println("   oneLetterCode  : " + oneLetterCode);
					System.out.println("   seq. index     : " + seqIndex); // index into complete polymer sequence ("SEQRES")
					System.out.println("   numAtoms       : " + numAtoms);
					System.out.println("   numBonds       : " + numBonds);
					System.out.println("   chemCompType   : " + chemCompType);
					System.out.println("   groupId        : " + groupId);
					System.out.println("   insertionCode  : " + insertionCode);
					System.out.println("   DSSP secStruct.: " + DsspSecondaryStructure.getDsspCode(secStruct).getOneLetterCode());
					System.out.println();

					groupIndex++; // update global group index
				}
				chainIndex++;
			}
		}
		System.out.println();
	}

	/**
	 * Traverses the basic hierarchy of the data structure and prints
	 * chain, entity, group, and atom information.
	 * 
	 * @param structure structure to be traversed
	 */
	public static void printChainEntityGroupAtomInfo(StructureDataInterface structure) {
		System.out.println("*** CHAIN ENTITY GROUP ATOM DATA ***");

		// create an index that maps a chain to its entity
		int[] chainToEntityIndex = getChainToEntityIndex(structure);

		// Global indices that point into the flat (columnar) data structure
		int chainIndex = 0;
		int groupIndex = 0;
		int atomIndex = 0;

		// Loop over models
		for (int i = 0; i < structure.getNumModels(); i++) {
			System.out.println("model: " + (i+1));

			// Loop over chains in a model
			for (int j = 0; j < structure.getChainsPerModel()[i]; j++) {

				// Print chain info
				String chainName = structure.getChainNames()[chainIndex]; // this is the chain name used in pdb files
				String chainId = structure.getChainIds()[chainIndex]; // this is also called asym_id in mmCIF		
				int groups = structure.getGroupsPerChain()[chainIndex];
				System.out.println("chainName: " + chainName + ", chainId: " + chainId + ", groups: " + groups);

				// Print entity info
				String entityType = structure.getEntityType(chainToEntityIndex[chainIndex]);		
				String entityDescription = structure.getEntityDescription(chainToEntityIndex[chainIndex]);
				String entitySequence = structure.getEntitySequence(chainToEntityIndex[chainIndex]); 
				System.out.println("entity type          : " + entityType);
				System.out.println("entity description   : " + entityDescription);
				System.out.println("entity sequence      : " + entitySequence);

				// Loop over groups in a chain
				for (int k = 0; k < structure.getGroupsPerChain()[chainIndex]; k++) {

					// get group data
					int groupId = structure.getGroupIds()[groupIndex]; // aka residue number
					char insertionCode = structure.getInsCodes()[groupIndex];
					int secStruct = structure.getSecStructList()[groupIndex];
					int seqIndex = structure.getGroupSequenceIndices()[groupIndex];

					// Unique groups (residues) are stored only once in a dictionary. 
					// We need to get the group type to retrieve group information
					int groupType = structure.getGroupTypeIndices()[groupIndex];	

					// retrieve group info from dictionary
					String groupName = structure.getGroupName(groupType);
					String chemCompType = structure.getGroupChemCompType(groupType);
					char oneLetterCode = structure.getGroupSingleLetterCode(groupType);
					int numAtoms = structure.getNumAtomsInGroup(groupType);
					int numBonds = structure.getGroupBondOrders(groupType).length;

					System.out.println("   groupName      : " + groupName);
					System.out.println("   oneLetterCode  : " + oneLetterCode);
					System.out.println("   seq. index     : " + seqIndex); // index into complete polymer sequence ("SEQRES")
					System.out.println("   numAtoms       : " + numAtoms);
					System.out.println("   numBonds       : " + numBonds);
					System.out.println("   chemCompType   : " + chemCompType);
					System.out.println("   groupId        : " + groupId);
					System.out.println("   insertionCode  : " + insertionCode);
					System.out.println("   DSSP secStruct.: " + DsspSecondaryStructure.getDsspCode(secStruct).getOneLetterCode());
					System.out.println("   Atoms          : ");

					// Loop over atoms in a group retrieved from the dictionary
					for (int m = 0; m < structure.getNumAtomsInGroup(groupType); m++) {

						// get atom info
						int atomId = structure.getAtomIds()[atomIndex];
						char altLocId = structure.getAltLocIds()[atomIndex];
						float x = structure.getxCoords()[atomIndex];
						float y = structure.getyCoords()[atomIndex];
						float z =structure.getzCoords()[atomIndex]; 
						float occupancy = structure.getOccupancies()[atomIndex];
						float bFactor = structure.getbFactors()[atomIndex];

						// get group specific atom info from the group dictionary
						String atomName = structure.getGroupAtomNames(groupType)[m];
						String element = structure.getGroupElementNames(groupType)[m];

						System.out.println("      " + atomId + "\t" + atomName + "\t" + altLocId + "\t" + x + "\t" + y 
								+ "\t" + z + "\t" + occupancy + "\t" + bFactor + "\t" + element);

						atomIndex++; // update global atom index
					}
					groupIndex++; // update global group index
				}
				chainIndex++;
			}
		}
		System.out.println();
	}

	/**
	 * Returns an array that maps a chain index to an entity index.
	 * @param structureDataInterface structure to be traversed
	 * @return index that maps a chain index to an entity index
	 */
	private static int[] getChainToEntityIndex(StructureDataInterface structure) {
		int[] entityChainIndex = new int[structure.getNumChains()];

		for (int i = 0; i < structure.getNumEntities(); i++) {
			for (int j: structure.getEntityChainIndexList(i)) {
				entityChainIndex[j] = i;
			}
		}
		return entityChainIndex;
	}
}