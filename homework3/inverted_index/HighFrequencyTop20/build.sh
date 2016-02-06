#!/bin/sh

JARS=`yarn classpath`

javac -classpath $JARS -d classes HighFrequencyTop.java XmlInputFormat.java
jar -cvf highfrequencytop.jar -C classes .