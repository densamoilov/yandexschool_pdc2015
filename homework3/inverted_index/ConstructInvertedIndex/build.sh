#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes InvertedIndex.java XmlInputFormat.java
jar -cvf invertedindex.jar -C classes .