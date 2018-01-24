# Installation of Prerequisites on Windows

The following libraries and tools are required to install mmtf-spark. If you do not have these tool installed, follow the instructions below.

### Install Java Development Toolkit (JDK)
To check the Java JDK installation, run the following command: 
```
javac -version
```
This should print: `javac 1.8.0_31` (version number should be 1.8.x or higher)

If you do not have a JDK or an older version, install the JDK
[Download JDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)

### Install UNIX Command Line Tools (Gow)
To check if you have access to UNIX command line tool, run the following command:
```
curl -h
```
This should print a help page.

If you do not have access to UNIX command line tool, install Gow. Gow provides access to UNIX command line 
tools such as curl or gzip in a Windows shell.

[Download Gow installer](https://github.com/bmatzelle/gow/releases) and run the installer.

### Install Git
Git is the version control system used to download the mmtf-spark repository from Github.
[Downlaod Git[(https://github.com/git-for-windows/git/releases/download/v2.16.1.windows.1/Git-2.16.1-64-bit.exe)


### Install Maven
The installation requires the [Maven](http://maven.apache.org/guides/getting-started/index.html#What_is_Maven) build tool. If you do not have Maven installed, follow the directions below.

[Download Maven](http://maven.apache.org/download.cgi) (zip version for Windows)

[Install Maven](http://maven.apache.org/install.html)

To check the Maven installation, run the following command:
```
$ mvn -v
```
