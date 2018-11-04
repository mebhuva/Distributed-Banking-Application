import sys
import socket
import pickle
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
branhList = []
initcount = 0
 

if __name__ == '__main__':
	
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
							



