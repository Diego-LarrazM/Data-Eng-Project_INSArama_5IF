#!bin/bash
openssl rand -base64 756 > ${PWD}/rs.key
chmod 0400 ${PWD}/rs.key
chown 999:999 ${PWD}/rs.key