# Installation on Windows

## Prerequisites
The following libraries and tools are required to install mmtf-spark. Except for Java, you need to choose an installation directory, for example your home directory C:\Users\USER_NAME. This directory is a placeholder for a location of your choice.

### Install Java SE Development Toolkit (JDK 1.8)
If you do not have the JDK or an older version, install the JDK 1.8.

[Download JDK 1.8 for Windows x64]http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and run the installer.

### Install UNIX Command Line Tools (Gow)
If you do not have access to UNIX command line tool, install Gow. It lets you run UNIX command line tools such as gzip, tar and curl in a Windows Command Prompt.

[Download Gow installer](https://github.com/bmatzelle/gow/releases) and run the installer.

### Install Git
The Git version control system is used to download repositories from Github.

[Download Git](https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe) and run the installer (choose all default options)


### Install Maven
The installation requires the [Maven](http://maven.apache.org/guides/getting-started/index.html#What_is_Maven) build tool. If you do not have Maven installed, follow the directions below.

[Download Maven](http://maven.apache.org/download.cgi) (apache-maven-3.5.2-bin.tar.gz) or newer version

Move the downloaded file to your install location (C:\Users\USER_NAME) and expand it using
gzip and tar.

```
cd C:\Users\USER_NAME
gzip -d apache-maven-3.5.2-bin.tar.gz
tar -xvf apache-maven-3.5.2-bin.tar
```

This will create the directory C:\Users\USER_NAME\apache-maven-3.5.2.

### Install Spark
[Download Spark](http://spark.apache.org/downloads.html)(Pre-built for Apache Hadoop 2.7 and later)

Move the downloaded file to your install location (C:\Users\USER_NAME) and expand it using
gzip and tar.

``` 
cd C:\Users\USER_NAME
gzip -d spark-2.2.1-bin-hadoop2.7.tgz
tar -xvf spark-2.2.1-bin-hadoop2.7.tar
```

### Install Hadoop winutils
Download winutils.exe
```
curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe?raw=true
```

Move wintutils.exe to the C:\Users\USER_NAME\spark-2.2.1-bin-hadoop2.7\bin directory.


# Install mmtf-spark
Create a Github account, if you don't have one. You may be asked for your Github username and password to clone the repository.

Clone the mmtf-spark repository and build the project using Maven.
```
cd C:\Users\USER_NAME
git clone https://github.com/sbl-sdsc/mmtf-spark.git
```

# Setup Environment Variables
1. In Search, search for and then select: System (Control Panel). [See also](https://www.java.com/en/download/help/path.xml)
2. Click the Advanced system settings link.
3. Click Environment Variables. In the section User Variables, find the PATH environment variable and select it. Click Edit. If the PATH environment variable does not exist, click New.
4. In the Edit User variable window, add the following values to the PATH variable. Use the exact version number and location of the downloaded files. 

| Variable      | Value                                 |
| ------------- |---------------------------------------|
| PATH          | C:\Program Files\Java\jdk1.8.0_162\bin |
| PATH          | C:\Users\USER_NAME\apache-maven-3.5.2\bin |
| PATH          | C:\Users\USER_NAME\spark-2.2.1-bin-hadoop2.7\bin |

5. Create the following new User variables.
| Variable      | Value                                 |
| ------------- |---------------------------------------|
| SPARK_HOME    | C:\Users\USER_NAME\spark-2.2.1-bin-hadoop2.7|
| JAVA_HOME     | C:\Program Files\Java\jdk1.8.0_162    |


5. Click OK. Close all remaining windows by clicking OK.

6. Reopen Command prompt window, and run your java code.


# Check your Prerequisites
Close all Command prompt windows. Then reopen a Command prompt window and type the following commands to check your setup.
```
javac  -version
git  --version
mvn  --version
winutils
echo %JAVA_HOME%
echo %SPARK_HOME%
```


# Build mmtf-spark
Run maven to build the mmtf-spark project and run the unit tests.
```
cd C:\Users\USER_NAME\mmtf-spark
mvn install
```

If the installation and testing is successful, you should see a message *Build Success*.