# Installation on MacOS

## Prerequisites
The following libraries and tools are required to install mmtf-spark. Except for Java, you need to choose an installation directory. This directory is a placeholder for a location of your choice.

### Install Java SE Development Toolkit (JDK 1.8)
If you do not have the JDK or an older version, install the JDK 1.8.

[Download JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and run the installer.

### Install Git
The Git version control system is used to download repositories from Github.

[Download Git](https://git-scm.com/download/mac) and run the installer (choose all default options)

### Install Maven
The installation requires the [Maven](http://maven.apache.org/guides/getting-started/index.html#What_is_Maven) build tool. If you do not have Maven installed, follow the directions below.

[Download Maven](http://maven.apache.org/download.cgi) (apache-maven-3.6.0-bin.tar.gz) or newer version

Expand the tar.gz file and move it to the /opt directory. If you are not an administrator on your computer, move the file to different install location.

```
sudo mv apache-maven-3.6.0 /opt
```

### Install Spark
[Download Spark](http://spark.apache.org/downloads.html) (Pre-built for Apache Hadoop 2.7 and later)

Expand the tar.gz file by double clicking on the file.

By default, SPARK logs at the INFO level, which leads to excessive log messages. Change the log level from INFO to ERROR

```
cd ./spark-2.4.0-bin-hadoop2.7/conf
cp log4j.properties.template log4j.properties
```

Then edit the log4j.properties file and change the level from INFO to ERROR:

`log4j.rootCategory=INFO, console` to `log4j.rootCategory=ERROR, console`


Then copy spark-2.4.0-bin-hadoop2.7 to the /opt directory. If you are not an administrator on your computer, move the file to different install location.

``` 
sudo mv spark-2.4.0-bin-hadoop2.7 /opt
```

# Install mmtf-spark
Clone the mmtf-spark repository and build the project using Maven.
```
cd INSTALL_PATH
git clone https://github.com/sbl-sdsc/mmtf-spark.git
```

# Setup Environment Variables
Add the following environment variables to your .bash_profile file.

Open .bash_profile with your favorite editor, e.g., using nano:

```
cd ~
nano .bash_profile

```

Add the following lines in your .bash_profile (adjust version numbers and locations as necessary):

```
export PATH=/opt/apache-maven-3.6.0/bin:/opt/spark-2.4.0-bin-hadoop2.7/bin:$PATH
export JAVA_HOME=`/usr/libexec/java_home`
export SPARK_HOME=/opt/spark-2.4.0-bin-hadoop2.7

```

Either close and reopen your terminal, or run the following command:

```
source ~/.bash_profile
```

# Check your Prerequisites
Type the following commands to check your setup.
```
javac  -version
git  --version
mvn  --version
echo $JAVA_HOME
echo $SPARK_HOME
```

# Build mmtf-spark
Run maven to build the mmtf-spark project and run the unit tests.
```
cd INSTALL_PATH\mmtf-spark
mvn install
```

If installation and testing are successful, a *Build Success* message will be printed.

# Download the PDB archive as Hadoop Sequence Files
MMTF-Hadoop Sequence files are available in two 
[representations](https://mmtf.rcsb.org/download.html):

* [![Download MMTF](http://img.shields.io/badge/download-MMTF_full-orange.svg?style=flat)](https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar) all-atom representation with 0.001Å coordinate precision, 0.01 B-factor and occupancy precision 

* [![Download MMTF Reduced](http://img.shields.io/badge/download-MMTF_reduced-orange.svg?style=flat)](https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar)  C-alpha atoms only for polypeptides, P-backbone atoms only for polynucleotides, all-atom representation for all other residue types, 
0.1Å coordinate precision, 0.1 B-factor and occupancy precision.

Extract the content from the .tar files by double clicking the file name.

Alternatively, the following command line tools can be used to download and extract the data:

```
curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
tar -xvf reduced.tar
```
or
```
wget https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
wget https://mmtf.rcsb.org/v1.0/hadoopfiles/reduced.tar
tar -xvf reduced.tar
```

Add the following lines in your .bash_profile:

```
export MMTF_FULL=INSTALL_PATH/full
export MMTF_REDUCED=INSTALL_PATH/reduced
```

# Increase Spark Driver Memory
If Spark throws an out of heap space error, increase the Spark driver memory (default 1G)

Add the following line in your .bash_profile and type: source .bash_profile

```
export SPARK_DRIVER_MEMORY=6G
```