import sys
import socket
import pickle
import logging
import random
import time
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
branhList = []
initcount = 0
snapshot_num = 1
 

if __name__ == '__main__':
	
	try:
		NoBranch = sum(1 for line in open(sys.argv[2]))
	except:
		logger.debug("Can not open the file")
		sys.exit(0)

	try:
		balance = int ( sys.argv[1] ) / int ( NoBranch )
	except:
		logger.debug("Input File is blank or Empty") 
		sys.exit(0)

	if NoBranch < 2 :
		logger.debug("Input File should have atleast two branches")
		sys.exit(0)
	
	with open(str(sys.argv[2])) as file:
        	for line in file:
                	data = line.strip().split(" ")
			d={}
			d["name"] = data[0]
			d["ip"]=data[1]
			d["port"]=int(data[2])
			branhList.append(d)

	#print branhList
	while True:
		if initcount == 0:
        		divide_Balance = int(sys.argv[1]) / len(branhList)
                	for branch in branhList:
           	        	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                		logger.debug("Initializing Branch:" + str(branch["name"]))
                    		s.connect((branch['ip'], branch['port']))
                    		init_branch = bank_pb2.InitBranch()
                    		init_branch.balance = divide_Balance                     
                    		for branchobj in branhList:
                        		branches = init_branch.all_branches.add()
                        		branches.name = branchobj['name']
                        		branches.ip = branchobj['ip']
                        		branches.port = branchobj['port']

                    		message = bank_pb2.BranchMessage()
                    		message.init_branch.CopyFrom(init_branch)

                    		s.sendall(pickle.dumps(message))
                    		s.close()
			initcount = 1
		time.sleep(10)


		logger.debug("Snapshot Id :- " +  str(snapshot_num))
		randomBranch = random.choice(branhList)
		InitSnapshotMsg = bank_pb2.InitSnapshot()
		InitSnapshotMsg.snapshot_id = int(snapshot_num)
		branchsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            	branchsocket.connect((randomBranch['ip'], randomBranch['port']))
		branchMessage = bank_pb2.BranchMessage()
                branchMessage.init_snapshot.CopyFrom(InitSnapshotMsg)
		branchsocket.sendall(pickle.dumps(branchMessage))
		branchsocket.close()
		time.sleep(10)


                totalCurrentBalance = 0
		for branchobj in branhList:
                	branches.name = branchobj['name']
                        branches.ip = branchobj['ip']
                        branches.port = branchobj['port']
			retriveSnapshotMessage = bank_pb2.RetrieveSnapshot()
	        	retriveSnapshotMessage.snapshot_id = int(snapshot_num)
			branchMessage1 = bank_pb2.BranchMessage()
                	branchMessage1.retrieve_snapshot.CopyFrom(retriveSnapshotMessage)
			branchsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            		branchsocket.connect((branchobj['ip'], branchobj['port']))
			branchsocket.sendall(pickle.dumps(branchMessage1))
			data = pickle.loads(branchsocket.recv(1024))		
			branchsocket.close()
			channel_state = ""
			newList = []
			for item in branhList:
				if item['name'] != branchobj['name']:
					newList.append(item)
			for branch , channelState in zip(newList, data.return_snapshot.local_snapshot.channel_state) :
					channel_state = channel_state + str(branch['name']) + " -> " + str(branchobj['name']) + " : " + str(channelState) + " "
					totalCurrentBalance = totalCurrentBalance + int(channelState)
			logger.debug(str(branchobj['name'])+ " : " + str(data.return_snapshot.local_snapshot.balance) + " , " + channel_state)
			totalCurrentBalance = totalCurrentBalance + data.return_snapshot.local_snapshot.balance

		#logger.debug("Total Balance " + str(totalCurrentBalance))
		snapshot_num += 1
