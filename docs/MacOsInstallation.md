# Installation on MacOS

* Under development *

## Prerequisites
The following libraries and tools are required to install mmtf-spark. Except for Java, you need to choose an installation directory. This directory is a placeholder for a location of your choice.

### Install Java SE Development Toolkit (JDK 1.8)
If you do not have the JDK or an older version, install the JDK 1.8.

[Download JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and run the installer.

### Install Git
The Git version control system is used to download repositories from Github.

[Download Git](https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe) and run the installer (choose all default options)

### Install Maven
The installation requires the [Maven](http://maven.apache.org/guides/getting-started/index.html#What_is_Maven) build tool. If you do not have Maven installed, follow the directions below.

[Download Maven](http://maven.apache.org/download.cgi) (apache-maven-3.5.2-bin.tar.gz) or newer version

Move the downloaded file to your install location (C:\Users\USER_NAME) and expand it using
gzip and tar in Command Prompt window.

```
cd INSTALL_DIRECTORY
gzip -d apache-maven-3.5.2-bin.tar.gz
tar -xvf apache-maven-3.5.2-bin.tar
```

This will create the directory INSTALL_DIRECTORY\apache-maven-3.5.2.

### Install Spark
[Download Spark](http://spark.apache.org/downloads.html) (Pre-built for Apache Hadoop 2.7 and later)

Move the downloaded file to your install location and expand it using
gzip and tar.

``` 
cd INSTALL_DIRECTORY
gzip -d spark-2.2.1-bin-hadoop2.7.tgz
tar -xvf spark-2.2.1-bin-hadoop2.7.tar
```

# Install mmtf-spark
Clone the mmtf-spark repository and build the project using Maven.
```
cd INSTALL_DIRECTORY
git clone https://github.com/sbl-sdsc/mmtf-spark.git
```

# Setup Environment Variables
Add the following environment variables to your .bash_profile file.

Open .bash_profile with your favorite editor, eg:

```
cd ~
vim ~/.bash_profile

```

Add the following lines in your bash_profile:

```
export SPARK_HOME=~/spark-2.2.0-bin-hadoop2.7  <Path to your spark>

```
TODO: which of those are needed for MacOS?

| Variable      | Value                                            |
| ------------- |--------------------------------------------------|
| PATH          | C:\Program Files\Java\jdk1.8.0_162\bin           |
| PATH          | C:\Users\USER_NAME\apache-maven-3.5.2\bin        |
| PATH          | C:\Users\USER_NAME\spark-2.2.1-bin-hadoop2.7\bin |

Create the following new User variables.

| Variable      | Value                                        |
| ------------- |----------------------------------------------|
| SPARK_HOME    | C:\Users\USER_NAME\spark-2.2.1-bin-hadoop2.7 |
| JAVA_HOME     | C:\Program Files\Java\jdk1.8.0_162           |

Source your .bash_profile

Either reopen your terminal, or run the following command:

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