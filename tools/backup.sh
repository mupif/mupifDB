#!/bin/bash

host="127.0.0.1"
port="27017"
dir=""

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

echo "MupifDB backup"
while getopts ":h:p:d:" opt; do
     case "$opt" in
          h)
                host=$OPTARG
                ;;
          P)
                port=$OPTARG
                ;;
          d)
                dir=$OPTARG
                ;;
          \?)
              echo "Invalid option: -$OPTARG" >&2
              echo "Usage: backup [-h hostname] [-p port] -d backupDir">&2
              exit 1
              ;;
          :)
              echo "Option -$OPTARG requires an argument." >&2
              echo "Usage: backup [-h hostname] [-p port] -d backupDir">&2
              exit 1
              ;;
     esac
done
                
if [ -z "$dir" ]
then
     echo "-d option required to set the backupDir"
     exit 1
else 
     echo "Using host=$host, port=$port, backupDir=$dir" >&2
     mongodump --host $host --port $port --out $dir
     echo "Done"
fi
