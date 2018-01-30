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

[Download Maven](http://maven.apache.org/download.cgi) (apache-maven-3.5.2-bin.tar.gz) or newer version

Expand the tar.gz file and copy it to the /opt directory. If you are not an administrator on your computer, copy the file to different install location.

```
sudo cp -r ./apache-maven-3.5.2 /opt
```

### Install Spark
[Download Spark](http://spark.apache.org/downloads.html) (Pre-built for Apache Hadoop 2.7 and later)

Expand the tar.gz file by double clicking on the file.

By default, SPARK logs at the INFO level, which leads to excessive log messages. Change the log level from INFO to ERROR

```
cd ./spark-2.2.1-bin-hadoop2.7/conf
cp log4j.properties.template log4j.properties
```

Then edit the log4j.properties file and change the level from INFO to ERROR:

`log4j.rootCategory=INFO, console` to `log4j.rootCategory=ERROR, console`


Then copy spark-2.2.1-bin-hadoop2.7 to the /opt directory. If you are not an administrator on your computer, copy the file to different install location.

``` 
sudo cp -r ./spark-2.2.1-bin-hadoop2.7 /opt
```

# Install mmtf-spark
Clone the mmtf-spark repository and build the project using Maven.
```
cd INSTALL_DIRECTORY
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
export PATH=/opt/apache-maven-3.5.2/bin:$PATH
export JAVA_HOME=`/usr/libexec/java_home`
export SPARK_HOME=/opt/spark-2.2.1-bin-hadoop2.7

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
cd INSTALL_DIRECTORY\mmtf-spark
mvn install
```

If installation and testing are successful, a *Build Success* message will be printed.