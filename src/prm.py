import time
import socket
import threading
import sys
import os
import pickle
import codecs
import base64
from queue import Queue


q = Queue() # shared queue 
quorum_size = 0
pid = 0
total_sites = 0
log = {} # { (Key: int:round; Value: LogOject:logObj), (''), ... }
current_round = 1
server_status_on = True

class LogObject():

    def __init__(self, filename, dictionary, decode):

        self.file = filename
        if (decode):
            self.dict = pickle.loads(codecs.decode(dictionary.encode(),'base64'))
        else:
            self.dict = dictionary

    def to_string(self):
        delimiter = " "
        serialized = str(self.file) + delimiter + base64.b64encode(pickle.dumps(self.dict)).decode()
        #print("SERIALIZED lobobj " + serialized)
        return serialized

    def print_obj(self):
        return str(self.file) + " " + str(self.dict)

class Ballot():
    def __init__(self, num, p_id, index):
        self.ballot_num = num
        self.process_id = p_id
        self.index = index

    def to_string(self):
        delimiter = " "
        return str(self.ballot_num) + delimiter + str(self.process_id) + delimiter +str(self.index)

    def get_round(self):
        return (self.index)

    def get_pid(self):
        return (self.process_id)

    def __ge__(self, other):
        if self.ballot_num > other.ballot_num:
            return True
        elif self.ballot_num == other.ballot_num:
            if self.process_id >= other.process_id:
                return True
            else:
                return False
        else:
            return False

    def __gt__(self, other):
        if self.ballot_num > other.ballot_num:
            return True
        elif self.ballot_num == other.ballot_num:
            if self.process_id > other.process_id:
                return True
            else:
                return False
        else:
            return False

class AcceptObj(Ballot):
    pass

class Messenger (object):
    global pid
    global total_sites
    global quorum_size
    def __init__(self, setup_data, cli_info):

        #setup_data = [[id,addr,port],...]
        self.in_sockets = []
        self.out_channel = [] # [site_id, ... ]
        self.out_sockets = {} # site_id: socket
        self.connections = {} # {site_id: (addr, port), ... }

        # set up connections
        for trip in setup_data:
            tmp_addr = (trip[1],int(trip[2]))
            self.connections[trip[0]] = tmp_addr
            if (trip[0]==pid):

                self.setup_server(tmp_addr,total_sites)

        time.sleep(5)        

        # set up connection to CLI
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(cli_info)
        self.cli_sock = sock

        # set up out_channel
        for trip in setup_data:
            self.setup_socket(trip[0])

        # listen for all
        counter = 0
        while True:
            try:
                conn, addr = self.server.accept()
                conn.setblocking(0)
                counter += 1
                self.in_sockets.append(conn)
                if counter == len(self.connections)+1:
                    break
            except BlockingIOError:
                pass


    def setup_socket(self, site_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.connections[site_id])
        self.out_sockets[site_id] = sock

    def setup_server(self, addr, num_sites):

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(addr)
        self.server.listen(num_sites+1)
        return None

    def send_cli(self, message):
        self.cli_sock.sendall(message.encode('utf-8'))

    def send_all(self):
        for s in self.out_sockets.values():
            s.sendall("hi/".encode('utf-8'))

    def recv_all(self):
        tmp_conn = {}

        for conn in self.in_sockets:
            tmp_conn[conn] = ""
        while True:
            for conn in self.in_sockets:
                try:
                    data = conn.recv(1024).decode('utf-8')
                    # tmp_conn[conn] += data
                    line = data.split("|")
                    print("line: " + str(line))
                    for x in range(len(line)):
                        tmp_conn[conn] += line[x]
                        if (x!=len(line)-1): # general case
                            q.put(tmp_conn[conn]+'--')
                            tmp_conn[conn] = ""




                    # if (data[-1:] == "|"):

                    #     temp = tmp_conn[conn]
                    #     temp = temp[:-1]
                    #     temp += "--"
                    #     q.put(temp)
                    #     print("PUT IN QUEUE: " + temp)
                    #     tmp_conn[conn] = ""
                    #print("the connection is " + str(conn))


                except BlockingIOError:
                    pass


    def send_prepare(self, ballot):
        '''
        Broadcasts a Prepare message to all Acceptors
        '''
        print ("Sending prepare: " + ballot.to_string())

        for s in self.out_sockets.values():
            message = "prepare " + ballot.to_string() + "|"
            print("sending size of encoded" + str(sys.getsizeof(message.encode('utf-8'))))
            print("sending size of encoded raw" + str(sys.getsizeof(message)))

            s.sendall(message.encode('utf-8'))

    def send_promise(self, ballot, accept_obj, log_obj, sender):
        '''
        Sends a Promise message to the specified Proposer
        '''
        #Promise looks like: "promise,ballot1 ballot2 ballot3,accept_class1 accept_class2 accept_class3,accepted_value"
        print("Sending promise: " + ballot.to_string())

        message = "promise " + ballot.to_string() + " " + accept_obj.to_string() + " " + log_obj.to_string() + "|"
        print("sending size of encoded" + str(sys.getsizeof(message.encode('utf-8'))))
        print("sending size of encoded raw" + str(sys.getsizeof(message)))
        self.out_sockets[sender].sendall(message.encode('utf-8'))

    def send_proposal(self, ballot, proposal_log_obj):
        '''
        Broadcasts an Accept! message to all Acceptors
        '''
        print ("Sending proposal: " + ballot.to_string())

        message = "accept " + ballot.to_string() + " " + proposal_log_obj.to_string() + "|"

        for s in self.out_sockets.values():
            s.sendall(message.encode('utf-8'))


    def send_accept(self, ballot, accept_log_obj):
        '''
        Broadcasts an Accepted message to all Learners
        '''
        print ("Sending accept: " + ballot.to_string())

        message = "accept " + ballot.to_string() + " " + accept_log_obj.to_string() + "|"
        for s in self.out_sockets.values():
            s.sendall(message.encode('utf-8'))


    def send_resume_req(self, round_num):
        global pid
        print ("Sending resumereq:")

        message = "resumereq " + str(round_num) + " " + str(pid) + "|"
        for s in self.out_sockets.values():
            s.sendall(message.encode('utf-8'))

    def send_log(self, req_pid):
        global log
        global current_round

        message = "log "
        message += str(current_round) + " "
        for obj in log.items():
            message += obj[1].to_string() + ","
        message += "|"

        self.out_sockets[req_pid].sendall(message.encode('utf-8'))

    # def send_resume_round_req(self):
    #     global pid
    #     print ("Resume Initiaited - sending round request from pid: " + str(pid))

    #     message = "roundReq " + str(pid)
    #     for s in self.out_sockets.values():
    #         s.sendall(message.encode('utf-8'))

    # def send_round_response(self, sender):
    #     global current_round
    #     global pid

    #     message = "roundResp " + str(current_round) + " " +  str(pid)
    #     print(message)
    #     self.out_sockets[sender].send_all(message.encode('utf-8'))

    # def send_log_req(self, current_round, sender):
    #     global pid

    #     message = "logReq " + str(current_round) + " " + str(pid)
    #     print( message)
    #     self.out_sockets[sender].send_all(message.encode('utf-8'))

    # def send_log_obj(self, sender_round, sender):
    #     global current_round
    #     global log

    #     message = "logResp "
    #     amountOfLogObj = 0
    #     messageBuffer = ""

    #     for x in range(sender_round, current_round): # obtain a string message "numOfObj roundNum 'fileName dictSearlized' roundNum 'fileName dictSearlized' ... "
    #         amountOfLogObj += 1
    #         messageBuffer += " " + str(x) + " " + log[x].to_string()
        
    #     message += str(amountOfLogObj) + messageBuffer
    #     print("sending log obs " + message)
    #     self.out_sockets[sender].send_all(message.encode('utf-8'))


class CliMethodObj(object):

    def __init__(self, msngr):
        self.messenger = msngr

    def replicate(self, filename):
        # Parse file into dict and then create log obj
        dictionary = {}
        with open(filename, 'r') as f:
            for line in f:
                key = line.split()[0]
                dictionary[key] = int(line.split()[1])

        return LogObject(filename, dictionary, False)


    def stopProcess(self):
        global server_status_on
        server_status_on = False

    def resumeProcess(self): 
        '''
        asks for current_round from each of the processes
        '''
        global server_status_on
        global current_round
        server_status_on = True

        self.messenger.send_resume_req(current_round)

    def recv_resume_req(self, round, sender_pid):
        '''
        Receive resume request from a process pid, send back whole log if current round greater 
        '''
        global current_round
        print("round vs current round:" + str(round) + " and " + str(current_round))
        if current_round >= round:
            self.messenger.send_log(sender_pid)

    def recv_log(self, round, raw_log):
        '''
        Receive and parse log and insert into own log
        '''
        global log
        global current_round

        if round < current_round:
            return

        counter = 1
        for str in raw_log:
            if str == "":
                continue
            filename, serialized = str.split()
            log_obj = LogObject(filename, serialized, True)
            log[counter] = log_obj

            # filename = log_obj.file
            # replicated_file = open(filename, "w")
            # dictionary = log_obj.dict
            # for key in dictionary:
            #     replicated_file.write(key + " " + str(dictionary[key]) + "\n")
                
            counter += 1


        current_round = round
    # def recv_round_req(self, sender_pid):
    #     self.messenger.send_round_response(sender_pid)

    # def recv_round_resp(self, sender_round, sender_pid):
    #     global current_round

    #     if (current_round >= sender_round): # dont do anything if you are ahead or equal to sender
    #         return

    #     else:
    #         current_round = sender_round # so this line can be done after log object is received or here
    #                                      # downside of doing it here is if request for log fails, we will have undefined behavior
    #                                      # downside of doing when received is that other request that comes it may overwrite values leading to undefined behavior as well
    #                                      # first option is safer for now until we come up with new solution
            
    #         self.messenger.send_log_request(current_round, sender_pid)

    # def recv_log_req(self, sender_round, sender_pid):
    #     '''
    #     send log objects differenced by your round and sender_round
    #     pre condition: sender_round will be lower round than self.current_round
    #     '''
    #     self.messenger.send_log_obj(sender_round, sender_pid)

    # def recv_log_obj(self, line):
    #     global log

    #     print("recv log obj: " + line)
    #     for i in range(0,int(line[1])):
    #         log[int(line[i*3+2])] = LogObject(line[i*3+3], line[i*3+4], True)
            

    def sendPrint(self):
        global log
        self.messenger.send_cli("print " + str(self.print_log()))

    def sendTotal(self, line):
        global log
        totalWordCount = 0

        for x in line[1:]:
            totalWordCount += sum(log[int(x)].dict.values())

        self.messenger.send_cli("total " + str(totalWordCount))

    def print_log(self):
        global log
        log_str = ""
        for obj in log.items():
            log_str += obj[1].file + ","
        return log_str

    def sendMerge(self, pos1, pos2):
        global log

        dict1 = log[pos1].dict
        dict2 = log[pos2].dict

        result = dict1.copy()
        for key in dict2:
            if key not in result:
                result[key] = dict2[key]
            else:
                result[key] += dict2[key]

        self.messenger.send_cli("merge " + str(result))

    
class Proposer (object):

    def __init__(self, msngr):

        
        self.messenger = msngr
        self.nextBallotNum = {} # { (Key: int:round; Value: int:nextBallotNum), (''), ... }
        self.proposedValues = {} # { (Key: int:round; Value: LogObj:proposedValue), (''), ... }
        self.myBallots = {} # { (Key: int:round; Value: Ballot:myBallot), (''), ... }
        self.promises = {} # { (Key: string: ballot; Value=[(acceptVal1, acceptObj1), (acceptVal2, acceptObj2)...], ('') }
                           # Keeps track of how many promises received from each proposal/ballot; 

              
    def checkForAllNull(self, ballot):
        for x in self.promises[ballot.to_string()]:
            if (x[0].file != "NULL"):
                return False
        return True

    def set_proposal(self, logObject):
        '''
        Finds lowest round in log, and proposes value to that round
        '''
        global current_round

        lowestRound = 1
        while True:
            if lowestRound not in log:
                break
            lowestRound += 1

        current_round = lowestRound
        self.nextBallotNum[current_round] = 1

        if lowestRound not in self.proposedValues:
            self.proposedValues[lowestRound] = logObject
        else:
            print("Undefined behavior in Proposer: set_proposal  -  lowest round has a proposed object currently going")
        return current_round

    def prepare(self):

        '''
        Sends a prepare request to all Acceptors as the first step in attempting to
        acquire leadership of the Paxos instance. 
        '''
        
        self.myBallots[current_round] = Ballot(self.nextBallotNum[current_round], pid, current_round)
        
        
        self.nextBallotNum[current_round] += 1
        self.promises[self.myBallots[current_round].to_string()] = []
        self.messenger.send_prepare(self.myBallots[current_round])
    


    def recv_promise(self, ballot, accept_obj, accepted_log_obj):
        '''
        Called when a Promise message is received from an Acceptor
        '''

        # Ignore the message if it's for an old proposal or we have already received
        # a response from this Acceptor
        if ballot.to_string() in self.promises and len(self.promises[ballot.to_string()]) >= quorum_size:
            return
        if ballot.get_round() < current_round: # if round lower than current, ignore
            return
        
        print("Received promise: " + ballot.to_string())

        self.promises[ballot.to_string()].append((accepted_log_obj,accept_obj))
        
        if len(self.promises[ballot.to_string()]) == quorum_size:
            # once you reach quorum size. check if all values received thus far were NULL
            if (self.checkForAllNull(ballot)):
                pass

            else:
                largestAcceptObj = AcceptObj(-1,-1,-1)
                largestVal = -1
                for x in self.promises[ballot.to_string()]:
                    if (x[1] > largestAcceptObj): 
                        largestVal = x[0]
                        largestAcceptObj = x[1]
                self.proposedValues[ballot.get_round()] = largestVal

            self.messenger.send_proposal(ballot, self.proposedValues[ballot.get_round()])

        
class Acceptor (object):

    def __init__(self, msngr):
        global pid
        self.messenger = msngr

        self.myAcceptValues = {}
        self.myBallots = {} # { key: int(round)  value: Ballot(0,0,0)}
        self.myAccepts = {} # { key: int(round)   value: Ballot(0,0,0)}
        self.accepted = {} # Holds ballots to check if sent before, keys=ballots, val=num received from
        

    def recv_prepare(self, from_id, ballot):
        '''
        Called when a Prepare message is received from a Proposer
        '''
        print("Received prepare: " + ballot.to_string())
        ballot_round = ballot.get_round()

        if (ballot_round < current_round): # if round of ballot received is less than current_round, you can ignore it
            return

        if ballot_round in self.myBallots: # ballot round already exists
            if ballot >= self.myBallots[ballot_round]:
                self.myBallots[ballot_round] = ballot
                self.messenger.send_promise(self.myBallots[ballot_round],self.myAccepts[ballot_round],self.myAcceptValues[ballot_round],from_id)

        else: # ballot where round does not exist yet
            self.myBallots[ballot_round] = ballot
            self.myAccepts[ballot_round] = AcceptObj(0,0,0)
            self.myAcceptValues[ballot_round] = LogObject("NULL",{}, False)
            self.messenger.send_promise(self.myBallots[ballot_round], self.myAccepts[ballot_round],self.myAcceptValues[ballot_round],from_id)



    def recv_propose(self, ballot, accept_log_obj):
        print ("Received proposed: " + ballot.to_string())

        if (ballot.get_round() < current_round): # starting to not know if this is necessary
            return

        if ballot >= self.myBallots[ballot.get_round()]:
            self.myAccepts[ballot.get_round()] = ballot
            self.myAcceptValues[ballot.get_round()] = accept_log_obj
            self.messenger.send_accept(self.myAccepts[ballot.get_round()], self.myAcceptValues[ballot.get_round()])


    def recv_accept(self, ballot, accept_log_obj):
        '''
        Called when a resolution is reached
        '''
        global pid

        # Ignore the message if it's for an old proposal or we have already received
        # a response from this Acceptor
        if ballot.to_string() in self.accepted and self.accepted[ballot.to_string()] >= quorum_size:
            return -1

        print("Received accept: " + ballot.to_string())

        # time.sleep(2) # In order to replicate simontaneous proposals in 1 round

        if ballot.get_round() in self.myBallots:

            if ballot >= self.myBallots[ballot.get_round()]: 

                if ballot.to_string() not in self.accepted: # first time 
                    self.myAccepts[ballot.get_round()] = ballot
                    self.myAcceptValues[ballot.get_round()] = accept_log_obj
                    self.messenger.send_accept(ballot, accept_log_obj)
                    self.accepted[ballot.to_string()] = 1


                else: 
                    self.accepted[ballot.to_string()] += 1

                if self.accepted[ballot.to_string()] == quorum_size:
                    log[ballot.get_round()] = accept_log_obj
                   
                    filename = accept_log_obj.file
                    replicated_file = open(filename, "w")
                    dictionary = accept_log_obj.dict
                    for key in dictionary:
                        replicated_file.write(key + " " + str(dictionary[key]) + "\n")
                    if (ballot.get_pid() != pid):
                        return ballot.get_round()


        else: # this is the case where you receive the accept message in round n before the prepare message in round n is received
              # self.myBallots[ballot.get_round()] has not existed yet. we create it when prepare is received
            self.myAccepts[ballot.get_round()] = ballot
            self.myAcceptValues[ballot.get_round()] = accept_log_obj
            self.messenger.send_accept(ballot, accept_log_obj)
            self.accepted[ballot.to_string()] = 1

        return -1 # default do nothing


class PRModule (object):

    def __init__(self, id, setup_file):

        global pid
        global total_sites
        global quorum_size
        pid = int(id)
        self.fileData = []
        self.allProposed = {} # {(Key: "round.pid", Value: logObj) , ... } holds all proposed values in order to check if we need to repropose after rounds
        cli_info = ("addr", -1)
        # reading setup file
        with open(setup_file, 'r') as f:
            total_sites = int(f.readline())

            for i in range(total_sites):
                line = f.readline().strip()
                tmp_pid,address,port = line.split()
                self.fileData.append((int(tmp_pid),address,int(port)))
            # Find CLI
            for i in range(total_sites):
                line = f.readline().strip()
                tmp_pid,address,port = line.split()
                if int(tmp_pid) == pid:
                    cli_info = (address,int(port))


        quorum_size = (total_sites//2) + 1
        self.messenger = Messenger(self.fileData, cli_info)
        self.proposer = Proposer(self.messenger)
        self.acceptor = Acceptor(self.messenger)
        self.cliObj = CliMethodObj(self.messenger)


    def listen(self):
        #Parse any incoming messages from the queue and call appropriate function
        global server_status_on
        global pid

        while True:
            received = q.get()
            received = received.split("--")
            for message in received:
                if message == "":
                    continue
                print ("RAW MESSAGE: " + message)
                line = message.split()


                # Commands to be executed even when server status is off
                if line[0] == "replicate":
                    line_second = message.split(" ",2)
                    log_obj = self.cliObj.replicate(line_second[1])
                    proposed_round = self.proposer.set_proposal(log_obj)
                    tmp_key = str(proposed_round) + '.' + str(pid)
                    self.allProposed[tmp_key] = (log_obj)
                    self.proposer.prepare()
                elif line[0] == "stop":
                    self.cliObj.stopProcess()
                elif line[0] == "resume":
                    self.cliObj.resumeProcess()
                elif line[0] == "total":
                    self.cliObj.sendTotal(line)
                    continue
                elif line[0] == "print":
                    self.cliObj.sendPrint()
                    continue
                elif line[0] == "merge":
                    self.cliObj.sendMerge(int(line[1]),int(line[2]))
                    continue

                # Commands to be executed only when server status is on
                elif (server_status_on):
                    if line[0] == "resumereq":
                        print("receiving resume reqeust")
                        self.cliObj.recv_resume_req(int(line[1]), int(line[2]))
                        continue
                    if line[0] == "log":
                        raw_log = message[6:].split(",")
                        self.cliObj.recv_log(int(line[1]), raw_log)
                        continue
                    # if line[0] == "roundReq":
                    #     self.cliObj.recv_round_req(int(line[1]))
                    #     continue
                    # if line[0] == "roundResp":
                    #     self.cliObj.recv_round_resp(int(line[1]), int(line[2]))
                    #     continue
                    # if line[0] == "logReq":
                    #     self.cliObj.recv_log_req(int(line[1]), int(line[2]))
                    #     continue
                    # if line[0] == "logResp":
                    #     self.cliObj.recv_log_obj(line)
                    #     continue
                    line_ballot = Ballot(int(line[1]), int(line[2]), int(line[3]))
                    if line[0] == "prepare":
                        self.acceptor.recv_prepare(int(line[2]), line_ballot)
                    if line[0] == "promise":
                        self.proposer.recv_promise(line_ballot, AcceptObj(int(line[2]), int(line[3]), int(line[4])), LogObject(line[-2], line[-1], True))
                    if line[0] == "propose":
                        self.acceptor.recv_propose(line_ballot,LogObject(line[4], line[5], True))
                    if line[0] == "accept":
                        #self.acceptor.recv_accept(line_ballot,LogObject(line[4], line[5], True))
                        round_accepted = self.acceptor.recv_accept(line_ballot,LogObject(line[4], line[5], True))
                        
                        if (int(round_accepted)!=-1):

                            tmp_key = str(round_accepted) + '.' + str(pid)

                            if tmp_key in self.allProposed:
                                proposed_round = self.proposer.set_proposal(self.allProposed[tmp_key])
                                tmp_key = str(proposed_round) + '.' + str(pid)
                                self.allProposed[tmp_key] = (log_obj)
                                self.proposer.prepare()                            



    def start(self):
        
        threading.Thread(target=self.messenger.recv_all).start() # constantly puts received data in global queue
        threading.Thread(target=self.listen).start()             # takes thigns from queue and processes it


if __name__ == '__main__':


    prm = PRModule(sys.argv[1], sys.argv[2])
    prm.start()
    # time for servers to connect
    exit(0)




