#!/bin/sh

REFERENCE_FILE=/home/user/work/software/ideaIC-145.844.1.tar.gz

PROJ=$PWD
TMP=/tmp/torrent

rm -rf $TMP
mkdir $TMP

mkdir $TMP/server
mkdir $TMP/client1
mkdir $TMP/client2
mkdir $TMP/client3

JAVA=$JAVA_HOME/bin/java

cd $PROJ
mvn assembly:assembly || exit

JAR=$PROJ/target/torrent-1.0-SNAPSHOT-jar-with-dependencies.jar
SERVER="$JAVA -cp $JAR ru.spbau.mit.TorrentTrackerMain"
CLIENT="$JAVA -cp $JAR ru.spbau.mit.TorrentClientMain"

echo SERVER

cd $TMP/server
$SERVER &
PID0=$!

sleep 5

echo CLIENT1

cd $TMP/client1

$CLIENT newfile 127.0.0.1 $REFERENCE_FILE 

$CLIENT list 127.0.0.1

$CLIENT run 127.0.0.1 &
PID1=$!

echo CLIENT2

cd $TMP/client2
$CLIENT get 127.0.0.1 0
$CLIENT run 127.0.0.1 &
PID2=$!

echo CLIENT3

cd $TMP/client3
$CLIENT get 127.0.0.1 0
$CLIENT run 127.0.0.1 &
PID3=$!

sleep 80

FILE_SUFFIX=downloads/0/`basename $REFERENCE_FILE`

echo CMP1
cmp --silent $TMP/client2/$FILE_SUFFIX $REFERENCE_FILE || echo "Bad file content for client 2"

echo CMP2
cmp --silent $TMP/client3/$FILE_SUFFIX $REFERENCE_FILE || echo "Bad file content for client 3"

echo KILL

kill $PID0 $PID1 $PID2 $PID3
kill -9 $PID0 $PID1 $PID2 $PID3
