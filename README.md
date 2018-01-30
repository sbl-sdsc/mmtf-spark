# mmtf-spark
MMTF-Spark is a Java open source project that provides APIs and sample applications for the scalable mining of 3D biomacromolecular structures, such as the Protein Data Bank (PDB) archive. MMTF-Spark uses Big Data technologies to enable high-performance parallel processing of macromolecular structures. MMTF-Spark use the following technology stack:
* [Apache Spark](https://spark.apache.org/) a fast and general engine for large-scale distributed data processing.
* [MMTF](https://mmtf.rcsb.org/) the Macromolecular Transmission Format for compact data storage, transmission and high-performance parsing
* [Hadoop Sequence File](https://wiki.apache.org/hadoop/SequenceFile) a Big Data file format for parallel I/O
* [Apache Parquet](https://parquet.apache.org/) a columnar data format to store dataframes
* [BioJava](http://biojava.org/) a framework for processing biological data

# Tutorials
The companion project [mmtf-workshop-2017](https://github.com/sbl-sdsc/mmtf-workshop-2017) offers an introduction to Apache Spark and in-depth tutorials and sample code how to use MMTF-Spark.

In addition, a Python version [MMTF-PySpark](https://github.com/sbl-sdsc/mmtf-pyspark) is under development. MMTF-PySpark offers demos as Jupyter notebooks as well as an experimental zero-install [Binder 2.0](https://elifesciences.org/labs/8653a61d/introducing-binder-2-0-share-your-interactive-research-environment) deployment of MMTF-PySpark.

# Installation

[MacOS and LINUX](docs/MacOsInstallation.md)

[Windows](docs/WindowsInstallation.md)

## Download the PDB archive as a Hadoop Sequence File
The mmtf-spark project reads the PDB archive in the [MMTF file format](https://doi.org/10.1371/journal.pcbi.1005575) from a [Hadoop Sequence File](https://wiki.apache.org/hadoop/SequenceFile). This file format enables high-performance, parallel processing of the entire PDB using [Apache Spark](https://spark.apache.org).
See [mmtf.rcsb.org](https://mmtf.rcsb.org/download.html) for more details.

An up to date file can be [downloaded](http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar). To extract the *full* directory from the *full.tar* archive, double click the file on *macOS* or use a tool such as [7-Zip](http://www.7-zip.org/) on *Windows*.

Alternatively, the following command line tools can be used download and extract the data:

```
curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
```
or
```
wget https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
```

## Running a Demo Application using spark-submit

Simple example of running a demo application.
[PolyPeptideChainStatistics](PolyPeptideChainStatistics.java)

```
spark-submit --master local --class edu.sdsc.mmtf.spark.mappers.demos.PolyPeptideChainStatistics  INSTALL_DIRECTORY/mmtf-spark/target/mmtf-spark-0.2.0-SNAPSHOT.jar
```

Example with command line arguments. This example read the PDB files
in an input directory (recursively) and creates an MMTF-Hadoop Sequence file.
[PdbToMmtfFull](PdbToMmtfFull.java)

```
spark-submit --master local  --class edu.sdsc.mmtf.spark.io.demos.PdbToMmtfFull  INSTALL_DIRECTORY/mmtf-spark/target/mmtf-spark-0.2.0-SNAPSHOT.jar PDB_FILE_DIRECTORY MMTF_HADOOP_FILE_DIRECTORY
```


## How to Cite this Work

Bradley AR, Rose AS, Pavelka A, Valasatava Y, Duarte JM, Prlić A, Rose PW (2017) MMTF - an efficient file format for the transmission, visualization, and analysis of macromolecular structures. PLOS Computational Biology 13(6): e1005575. doi: [10.1371/journal.pcbi.1005575](https://doi.org/10.1371/journal.pcbi.1005575)

Valasatava Y, Bradley AR, Rose AS, Duarte JM, Prlić A, Rose PW (2017) Towards an efficient compression of 3D coordinates of macromolecular structures. PLOS ONE 12(3): e0174846. doi: [10.1371/journal.pone.01748464](https://doi.org/10.1371/journal.pone.0174846)

Rose AS, Bradley AR, Valasatava Y, Duarte JM, Prlić A, Rose PW (2016) Web-based molecular graphics for large complexes. In Proceedings of the 21st International Conference on Web3D Technology (Web3D '16). ACM, New York, NY, USA, 185-186. doi: [10.1145/2945292.2945324](https://doi.org/10.1145/2945292.2945324)

## Funding
This project is supported by the National Cancer Institute of the National Institutes of Health under Award Number U01CA198942. The content is solely the responsibility of the authors and does not necessarily represent the official views of the National Institutes of Health.
