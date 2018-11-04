import sys
import socket

import pickle
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2



branchName = ""
branchBalance = 0
branchLst = []

if __name__ == '__main__':
	branchName = sys.argv[1]
	serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serversocket.bind((socket.gethostbyname(socket.gethostname()), int(sys.argv[2])))
	serversocket.listen(5)
	logger.debug("\nWaiting for connection... Listening on "+str(socket.gethostbyname(socket.gethostname())+":"+ sys.argv[2]))
	while 1:
        	(clientsocket, address) = serversocket.accept()
		data = pickle.loads(clientsocket.recv(1024))
		if data.HasField("init_branch") :
			branchBalance = data.init_branch.balance
			for branch in data.init_branch.all_branches:
                        	if branch.name != branchName:
                        		branchLst.append(branch)

                print branchLst