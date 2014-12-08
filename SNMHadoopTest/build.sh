#!/bin/bash
rm -rf snmhadooptest.jar
rm -rf buildH/*
/usr/java/jdk1.7.0_55-cloudera/bin/javac -cp ./src/:/opt/cloudera/parcels/CDH/lib/hadoop/*:/root/SNMHadoopTest/lib/* src/SNMHadoopTest.java -d ./buildH
/usr/java/jdk1.7.0_55-cloudera/bin/jar -cvfm snmhadooptest.jar MANIFEST.MF -C ./buildH . ./lib/post*