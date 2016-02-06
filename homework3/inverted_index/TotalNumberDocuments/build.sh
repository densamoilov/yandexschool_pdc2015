#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes TotalNumberDocuments.java XmlInputFormat.java
jar -cvf totalnumberdocuments.jar -C classes .