import sys
import socket
import random
from threading import Thread
import pickle
import logging
import time
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2



branchName = ""
branchBalance = 0
branhList = []
moneyTransfer = False



def MoneyTransfer():
	global branchBalance
	global branhList
	global moneyTransfer
	while moneyTransfer and branchBalance>50:
		#print "branch la balance va"+str(branchBalance)
		randomBranch = random.choice(branhList)
		print randomBranch.ip
		print randomBranch.port
		MoneyTransferMsg = bank_pb2.Transfer()
		MoneyTransferMsg.money = (int) ((branchBalance* random.randint(1,5)) /100)
		branchBalance = branchBalance - MoneyTransferMsg.money
		branchsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            	branchsocket.connect((randomBranch.ip, randomBranch.port))
		branchMessage = bank_pb2.BranchMessage()
                branchMessage.transfer.CopyFrom(MoneyTransferMsg)
		branchsocket.sendall(pickle.dumps(branchMessage))
		branchsocket.close()
	time.sleep(random.randrange(1, 5))




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
                        		branhList.append(branch)

                print branhList
		print "branchBalance...."+ str(branchBalance) 
		moneyTransfer = True
		if len(branhList) > 0:
			thread = Thread(target = MoneyTransfer)
                	thread.daemon = True
                	thread.start()

		if data.HasField("transfer") :
			branchBalance = branchBalance + data.transfer.money
			print "branchBalance Updated to...."+ str(branchBalance) 