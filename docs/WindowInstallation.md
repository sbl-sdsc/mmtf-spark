# Installation on Windows

The following libraries and tools are required to install mmtf-spark. If you do not have these tool installed, follow the instructions below.

## Prerequisites

### Install Java Development Toolkit (JDK)
To check your current Java JDK installation, run the following command: 
```
javac --version
```
This should print: `javac 1.8.0_31` (version number should be 1.8.xxxx or higher)

If you do not have the JDK or an older version, install the JDK

[Download JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and run the installer.

Set environmental variables:
* User variable:
** Variable: JAVA_HOME
** Value: C:\Program Files\Java\jdk1.8.xxxx
* System variable:
** Variable: PATH
** Value: C:\Program Files\Java\jdk1.8.xxxx\bin

Open a new Windows Command Prompt (cmd) and type
```
echo %JAVA_HOME%
```
This should display the path to the Java installation.

### Install UNIX Command Line Tools (Gow)
To check if you have access to UNIX command line tool, run the following command:
```
curl --version
```

If you do not have access to UNIX command line tool, install Gow. It lets you run UNIX command line tools such as gzip, tar and curl in a Windows Command Prompt.

[Download Gow installer](https://github.com/bmatzelle/gow/releases) and run the installer.

### Install Git
To check if you have git installed, run the following command:
```
git --version
```
This should display the version number, e.g., `git version 2.16.1.windows.1`.

Git is the version control system used to download the mmtf-spark repository from Github.

[Download Git](https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe) and run the installer (choose all default options)


### Install Maven
To check the Maven installation, run the following command:
```
mvn --version
```

The installation requires the [Maven](http://maven.apache.org/guides/getting-started/index.html#What_is_Maven) build tool. If you do not have Maven installed, follow the directions below.

[Download Maven](http://maven.apache.org/download.cgi)(apache-maven-3.5.2-bin.tar.gz) or newer version

Copy this file to a suitable location, e.g., C:\Users\username\apache-maven-3.5.2-bin.tar.gz


```
gzip -d apache-maven-3.5.2-bin.tar.gz
tar -xvf apache-maven-3.5.2-bin.tar
```
Set environmental variables:
* User variable:
** Variable: JAVA_HOME
** Value: C:\Program Files\Java\jdk1.8.xxxx
* System variable:
** Variable: PATH
** Value: C:\Program Files\Java\jdk1.8.xxxx\bin

## Install Spark
[Download Spark](http://spark.apache.org/downloads.html)(Pre-built for Apache Hadoop 2.7 and later)

Copy the downloaded file to a suitable location and expand it:

``` 
cp spark-2.2.1-bin-hadoop2.7.tgz C:\Users\username
cd C:\Users\username
gzip -d spark-2.2.1-bin-hadoop2.7.tgz
tar -xvf spark-2.2.1-bin-hadoop2.7.tar
```
Set environmental variables:
* User variable:
** Variable: SPARK_HOME
** Value: C:\Users\username\spark-2.2.1-bin-hadoop2.7
* System variable:
** Variable: PATH
** Value: C:\Users\username\spark-2.2.1-bin-hadoop2.7\bin

## Install winutils.exe
To check winutils installation, run the following command
```
winutils
```
This should display the winutils help file.

Download winutils.exe
```
curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe?raw=true

## Install mmtf-spark
Clone (or fork) the mmtf-spark repository and build the project using Maven.
```
git clone https://github.com/sbl-sdsc/mmtf-spark.git
cd mmtf-spark
mvn install
```