#!/usr/bin/python

import socket
import sys
from socket import *
import argparse

Host = ''
p = argparse.ArgumentParser()
p.add_argument("Port", type=int)
args = p.parse_args()
Port = args.Port
Socket = socket(AF_INET, SOCK_STREAM)
Socket.bind((Host, Port))
Socket.listen(1)

if len(sys.argv) != 2:
    print 'Usage: Invalid number of parameters'
    sys.exit();

while True:
        conn, addr = Socket.accept()
        try:
                request = conn.recv(1024)
                response = request.split()[1];
                fileName = response.replace('/', '')
                file = open(fileName)
                output = file.read()
                conn.send('HTTP/1.1 200 OK\n\n')
                conn.send(output)
                conn.close()
        except IOError:
                conn.send('HTTP/1.1 404 File not found\n\n')
                conn.send('404 Error: File not found')
                conn.close()


