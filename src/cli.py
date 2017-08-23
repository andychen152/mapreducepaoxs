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
        self.out_sockets = {} # site_id: socket
        self.connections = {} # {site_id: (addr, port), ... }

        self.setup_server(cli_info, 5)

        # set up connections
        for info in setup_data:
            tmp_addr = (info[2],int(info[3]))
            if info[0] == "prm":
            	self.connections["prm"] = tmp_addr
            if info[0] == "map1":
                self.connections["map1"] = tmp_addr
            if info[0] == "map2":
                self.connections["map2"] = tmp_addr
            if info[0] == "red":
                self.connections["red"] = tmp_addr


        time.sleep(3)        
        
        self.setup_socket("prm")       
        self.setup_socket("map1")     
        self.setup_socket("map2")     
        self.setup_socket("red")  
            
        # listen for all modules
        counter = 0
        while True:
           try:
               conn, addr = self.server.accept()
               conn.setblocking(0)
               counter += 1
               self.in_sockets.append(conn)
               if counter == 4: # Listen for num modules ( just prm for now)
                   break
           except BlockingIOError:
               pass

        

    def setup_socket(self, module_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("mod id is " + str(module_id))
        sock.connect(self.connections[module_id])
        self.out_sockets[module_id] = sock

    def setup_server(self, addr, num_sites):

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(addr)
        self.server.listen(num_sites)

        return None

    def send_prm(self, message):
        message += "|"
       	self.out_sockets["prm"].sendall(message.encode('utf-8'))

    def send_map(self, message, mapperNum):
        print (mapperNum)
        if (not(mapperNum == 1 or mapperNum == 2)):
            print("ERROR: Received invalid mapperNum")
        else:
            self.out_sockets["map"+str(mapperNum)].sendall(message.encode('utf-8'))

    def send_reducer(self, message):
        self.out_sockets["red"].sendall(message.encode('utf-8'))

    def recv_all(self):
        while True:
            for conn in self.in_sockets:
                try:
                    data = conn.recv(1024)
                    listen_q.put(data)
                    listen_q.task_done()
                except BlockingIOError:
                    pass


class CLI():
    def __init__(self, messenger):
        self.messenger = messenger
        
    def replicate(self, filename):
        word_dict = {}
        with open(filename, 'r') as f:
            for line in f:
                line = line.split()
                word_dict[line[0]] = int(line[1])
        dict_str = str(word_dict)
        messenger.send_prm("replicate " + filename) #+ dict_str + "/")

        
    def stop(self):
        messenger.send_prm("stop")

    def resume(self):
        messenger.send_prm("resume")

    def total(self, message):
        messenger.send_prm("total " + message )

    def print(self):
        messenger.send_prm("print")

    def merge(self, message):
        messenger.send_prm("merge " + message )

    def map(self, fileName):
        # find offsets
        content = ""
        with open(fileName, 'r') as content_file:
            content = content_file.read()

        middleIndex = (len(content)-1)//2
        print(middleIndex)

        while (content[middleIndex] != ' '):
            middleIndex += 1

        offset_1 = 0
        size_1 = middleIndex-1
        offset_2 = middleIndex+1
        size_2 = len(content)-offset_2

        messenger.send_map("map " + fileName + " " + str(offset_1) + " " + str(size_1), 1)
        messenger.send_map("map " + fileName + " " + str(offset_2) + " " + str(size_2), 2)

    def reduce(self, filenames):
        message = "reduce"
        for filename in filenames:
            message += ' '
            message += filename 
        messenger.send_reducer(message)


    def takeCommands(self):
        while True:
            print("Input Command: ")
            rawLine = stdin.readline()
            line = rawLine.split()
            if line == "": # safegarud
                continue
            command = line[0]
            print("executing " + command)
            if command.strip() == "replicate":
                self.replicate(line[1])
                continue
            elif command.strip() == "stop": # done
                self.stop()
                continue
            elif command.strip() == "resume": # in prog
                self.resume()
                continue
            elif command.strip() == "total":
                self.total(rawLine[6:])
                continue
            elif command.strip() == "print":
                self.print()
                continue
            elif command.strip() == "merge":
                self.merge(line[1]+ " " + line[2])
                continue
            elif command.strip() == "map":
                self.map(line[1])
            elif command.strip() == "reduce":
                self.reduce(line[1:])
            else:
                print("Error: invalid command")
                continue

    
    def listen(self):
    #Parse any incoming messages from the queue and call appropriate function
    
        while True:
            received = listen_q.get().decode('utf-8')
            received = received.split("/")
            for message in received:
                if message == "":
                    continue
                line = message.split()
                # Commands to be executed from modules
                if line[0] == "print":
                    print(message[6:])
                if line[0] == "total":
                    print(message[6:])
                if line[0] == "merge":
                    print(message[6:])


if __name__ == '__main__':

    pid = int(sys.argv[1])
    setup_file = sys.argv[2]
    fileData = []
    prm_addr = (-1,"ip",-1)
    self_addr = (-1,"ip",-1)
    cli_info = (-1,-1)
    # reading setup file
    # Setup file has first n lines for prm, second n lines for clis
    with open(setup_file, 'r') as f:
        total_sites = int(f.readline())

        '''
        can do this all in 1 forloop later on if we really care about code length
        '''

        # inputting prm into fileData
        for i in range(total_sites): 
            line = f.readline().strip()
            if line == "": # safegaurd
                continue
            tmp_pid,address,port = line.split()
            if (int(tmp_pid) == pid):
                fileData.append(("prm",int(tmp_pid),address,int(port)))

        # inputting cli into fileData
        for i in range(total_sites):
            line = f.readline().strip()
            if line == "": # safegarud
                continue
            tmp_pid,address,port = line.split()
            if (int(tmp_pid) == pid):
                fileData.append(("cli",int(tmp_pid),address,int(port)))
                cli_info = (address,int(port))

        # inputting mapper_1 and mapper_2 into fileData
        for i in range(total_sites*2):
            line = f.readline().strip()
            if line == "": # safegarud
                continue
            tmp_pid,address,port = line.split()
            if (int(tmp_pid) == (2*pid)-1):
                fileData.append(("map1",int(tmp_pid),address,int(port)))
            if (int(tmp_pid) == 2*pid):
                fileData.append(("map2",int(tmp_pid),address,int(port)))

        # inputting reducer into fileData
        for i in range(total_sites):
            line = f.readline().strip()
            if line == "": # safegarud
                continue
            tmp_pid,address,port = line.split()
            if (int(tmp_pid) == pid):
                fileData.append(("red",int(tmp_pid),address,int(port)))

        # check for successful setup -> fileData parse
        if (len(fileData) != 5 or fileData[0][0] != "prm" or fileData[1][0] != "cli" or fileData[2][0] != "map1" or fileData[3][0] != "map2" or fileData[4][0] != "red"):
            print ("ERROR: setup file parse unsuccessful")
            print (len(fileData))


    messenger = Messenger(fileData, cli_info)
    cli = CLI(messenger)
    threading.Thread(target=cli.takeCommands).start()
    threading.Thread(target=messenger.recv_all).start() # constantly puts received data in global queue
    threading.Thread(target=cli.listen).start()   

