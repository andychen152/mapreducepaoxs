import time
import socket
import threading
from sys import stdin
import sys
import os
from queue import Queue

listen_q = Queue()

class Messenger (object):

    def __init__(self, setup_data, my_addr):

        self.in_sockets = []
        self.out_channel = [] # [site_id, ... ]
        self.out_sockets = {} # site_id: socket
        self.connections = {} # {site_id: (addr, port), ... }

        self.setup_server(my_addr, 1) # listen only for cli

        # set up connections
        for info in setup_data:
            tmp_addr = (info[2],int(info[3]))
            if info[0] == "cli":
            	self.connections["cli"] = tmp_addr

        time.sleep(3)        

        # set up out_channel
        self.setup_socket("cli")
            
        # listen for all modules
        counter = 0
        while True:
           try:
               conn, addr = self.server.accept()
               conn.setblocking(0)
               counter += 1
               self.in_sockets.append(conn)
               if counter == 1: # Listen for CLI
                   break
           except BlockingIOError:
               pass
    

    def setup_socket(self, module_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.connections[module_id])
        self.out_sockets[module_id] = sock

    def setup_server(self, addr, num_sites):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(addr)
        self.server.listen(num_sites)
        return None

    def send_cli(self):
        self.out_sockets["cli"].sendall("Finished Map".encode('utf-8'))

    def recv_all(self):
        while True:
            for conn in self.in_sockets:
                try:
                    data = conn.recv(1024)
                    listen_q.put(data)
                except BlockingIOError:
                    pass

class Mapper(object):

    def __init__(self,id,messenger):
        self.id = id
        self.messenger = messenger


    def map(self, filename, offset, size):
        f = open(filename, "r") 

        dictionary = {} 

        f.seek(offset)
        pointer = offset
        word = ""
        size += offset
        while pointer <= size:
            c = f.read(1)
            word += c
            if c == " " or c == '' or pointer==size:
                word = word.strip()
                if word not in dictionary and word != "" and word != " ":
                    dictionary[word] = 1
                elif word != "" and word != " ":
                    dictionary[word] = dictionary[word] + 1
                word = ""
            pointer += 1


        # Write to new file filename_I_mapperid
        new_filename = filename + "_I_" + str(self.id)
        new_file = open(new_filename, "w")
        for key in dictionary:
            new_file.write(key + " " + str(dictionary[key]) + "\n")

        messenger.send_cli()

    def listen(self):

    # Parse any incoming messages from the queue and call appropriate function
        while True:
            received = listen_q.get().decode('utf-8')
            received = received.split("/")
            for message in received:
                if message == "":
                    continue
                line = message.split()
                # Commands to be executed from modules
                if line[0] == "map":
                    self.map(line[1], int(line[2]), int(line[3]))
                else:
                    print ("ERROR: mapper received invalid command from CLI")



if __name__ == '__main__':

    pid = int(sys.argv[1])
    setup_file = sys.argv[2]
    fileData = []
    my_info = (-1,-1)
    # reading setup file
    # Setup file has first n lines for prm, second n lines for clis
    with open(setup_file, 'r') as f:
        total_sites = int(f.readline())

        for i in range(total_sites):
            line = f.readline()

        for i in range(total_sites):
            line = f.readline().strip()
            if line == "": # safegaurd
                continue
            tmp_pid,address,port = line.split()
            if (int(tmp_pid) == ((pid + 1)//2) ):
                fileData.append(("cli",int(tmp_pid),address,int(port)))
        for i in range(total_sites*2):
            line = f.readline().strip()
            if line == "": # safegaurd
                continue
            tmp_pid,address,port = line.split()
            if (int(tmp_pid) == pid):
                my_info = (address,int(port))        

    messenger = Messenger(fileData, my_info)
    mapper = Mapper(pid, messenger)

    threading.Thread(target=messenger.recv_all).start() # constantly puts received data in global queue
    threading.Thread(target=mapper.listen).start()   






