#!/bin/bash
echo "Cleaning up resources"
pidof procs | xargs kill -9 2> /dev/zero 
pidof node | xargs kill -9 2> /dev/zero

# Number of nodes
for i in {0..100}
do
#/home/thomas/go/src/github.com/tomcat-bit/lohpi
   fusermount -u /home/thomas/go/src/github.com/tomcat-bit/lohpi/fs_mount/storage_nodes/node_$i 2> /dev/zero
   fusermount -u /tmp/storage_nodes/node_$i 2> /dev/zero
done

# Remove FUSE points
rm -rf /home/thomas/go/src/github.com/tomcat-bit/lohpi/fs_mount/storage_nodes/ /tmp/storage_nodes
rm *logfile
rm -rf /home/thomas/go/src/github.com/tomcat-bit/lohpi/policy_store
mkdir /home/thomas/go/src/github.com/tomcat-bit/lohpi/policy_store
git init /home/thomas/go/src/github.com/tomcat-bit/lohpi/policy_store
