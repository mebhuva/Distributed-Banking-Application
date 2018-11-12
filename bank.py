import sys
import socket
import random
from threading import Thread
import pickle
import logging
import time
from threading import Lock
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2


TOTAL_BALANCE = 0
moneyTransfer = False
branchName = ""
branhList = []
timeLimit = 0

class Bank(object):

    snapshotList = []
    markerMsgChnlState = {}
    markerMsgBalance = {}
    critical_section_lock = Lock()

    def MoneyTransfer(self):
	global TOTAL_BALANCE
	global branchName
	global branhList
	global moneyTransfer
	global timeLimit
	while True:
		logger.debug("Money Transfer.............")
		logger.debug("Money Transfer Status = "+str(moneyTransfer) +"  Current Branch Balance = "+ str(TOTAL_BALANCE))
		if moneyTransfer == True and TOTAL_BALANCE>50:
			randomBranch = random.choice(branhList)
			print randomBranch.ip
			print randomBranch.port
			MoneyTransferMsg = bank_pb2.Transfer()
			MoneyTransferMsg.money = (int) ((TOTAL_BALANCE* random.randint(1,5)) /100)
			MoneyTransferMsg.src_branch = branchName
			with self.critical_section_lock:
				if moneyTransfer == True :
					TOTAL_BALANCE = TOTAL_BALANCE - MoneyTransferMsg.money
			branchsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            		branchsocket.connect((randomBranch.ip, randomBranch.port))
			branchMessage = bank_pb2.BranchMessage()
                	branchMessage.transfer.CopyFrom(MoneyTransferMsg)
			branchsocket.sendall(pickle.dumps(branchMessage))
			branchsocket.close()
			print "Time limit +++++++ " + str(timeLimit)
			time.sleep(random.uniform(0, 5))

    def SendMarkers(self,snapshot_id):
	global branhList
	global moneyTransfer
	global branchName
	moneyTransfer  = False
	markerMessage = bank_pb2.Marker()
        markerMessage.snapshot_id = snapshot_id
	markerMessage.src_branch = branchName
        branchMessage = bank_pb2.BranchMessage()
        branchMessage.marker.CopyFrom(markerMessage)
	for branch in branhList:
        	markerSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                markerSocket.connect((branch.ip, branch.port))
                markerSocket.sendall(pickle.dumps(branchMessage))
                markerSocket.close()
	moneyTransfer  = True

    def bankhandle(self,data,branchNameIn,clientsocket,timelimit):
	global TOTAL_BALANCE
	global branhList
        global moneyTransfer
	global branchName
	global timeLimit
	branchName = branchNameIn
	if data.HasField("init_branch") :
			TOTAL_BALANCE = data.init_branch.balance
			for branch in data.init_branch.all_branches:
                        	if branch.name != branchName:
                        		branhList.append(branch)
			logger.debug("Branch List initialized .....")
			logger.debug(branhList)
			moneyTransfer = True 
			timeLimit = (timelimit/1000.0)
			thread = Thread(target = self.MoneyTransfer)
                	thread.daemon = True
                	thread.start()

	elif data.HasField("transfer") :
			if len(self.snapshotList) > 0 :
                        	snapshot_num = self.snapshotList [-1]
			logger.debug("Transfer Message Recieved.....")
                        logger.debug("if incoming message need to be recorded or Store it in current branch balance")
                        if len(self.snapshotList) > 0 and self.markerMsgChnlState[ snapshot_num , data.transfer.src_branch ][0] == True and self.markerMsgBalance != 0:
				logger.debug("Recording Transfer balance in marker message channel state")
                                self.markerMsgChnlState[ snapshot_num , data.transfer.src_branch ] = (True , int(data.transfer.money))
			else:	
        		        with self.critical_section_lock:
                 		       TOTAL_BALANCE = TOTAL_BALANCE + data.transfer.money
			print "branchBalance Updated to...."+ str(TOTAL_BALANCE)
			
		
	elif data.HasField("init_snapshot") :
			time.sleep(2)
			moneyTransfer  = False
                	logger.debug("Recording initial snapshot")
                	self.snapshotList.append(data.init_snapshot.snapshot_id)
                	self.markerMsgBalance[data.init_snapshot.snapshot_id] = TOTAL_BALANCE
                	for branch in branhList:
      				logger.debug("Started recording on all the incoming channels")
                        	self.markerMsgChnlState[ data.init_snapshot.snapshot_id , branch.name ] = (True , 0 )

                	logger.debug("Sending Markers to all the channels")
                	thread = Thread(target = self.SendMarkers(data.init_snapshot.snapshot_id))
                	thread.daemon = True
                	thread.start()
		
	elif data.HasField("marker") :
			moneyTransfer  = False
			logger.debug("Marker message received")
			if data.marker.snapshot_id not in self.snapshotList :
				self.snapshotList.append(data.marker.snapshot_id)
                        	self.markerMsgBalance[data.marker.snapshot_id] = TOTAL_BALANCE
				self.markerMsgChnlState[ data.marker.snapshot_id , data.marker.src_branch] = (False , 0 )
                        	for branch in branhList:
					if branch.name != data.marker.src_branch :
                                		self.markerMsgChnlState[ data.marker.snapshot_id , branch.name ] = (True , 0 )
                        	self.SendMarkers(data.marker.snapshot_id)
                	else:
                        	amount = self.markerMsgChnlState[data.marker.snapshot_id , data.marker.src_branch][1]
                              	self.markerMsgChnlState[data.marker.snapshot_id , data.marker.src_branch] = (False , amount )
			moneyTransfer  = True	
	elif data.HasField("retrieve_snapshot") :
			logger.debug("Retriving snapshot")
			returnSnapshotMessage = bank_pb2.ReturnSnapshot.LocalSnapshot()
                	returnSnapshotMessage.snapshot_id = int(data.retrieve_snapshot.snapshot_id)
                	returnSnapshotMessage.balance = int(self.markerMsgBalance[data.retrieve_snapshot.snapshot_id])
                	for returnBranch in branhList:
                        	amount = self.markerMsgChnlState[data.retrieve_snapshot.snapshot_id , returnBranch.name][1]
				with self.critical_section_lock:
					TOTAL_BALANCE = TOTAL_BALANCE + amount 
				returnSnapshotMessage.channel_state.append(int(amount))

			branchMessage = bank_pb2.ReturnSnapshot()
                	branchMessage.local_snapshot.CopyFrom(returnSnapshotMessage)
			branchmessage = bank_pb2.BranchMessage()
			branchmessage.return_snapshot.CopyFrom(branchMessage)
			logger.debug("Returning Snapshot : "+ str(branchmessage))
			clientsocket.sendall(pickle.dumps(branchmessage))


if __name__ == '__main__':
	serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serversocket.bind((socket.gethostbyname(socket.gethostname()), int(sys.argv[2])))
	serversocket.listen(5)
	logger.debug("\nWaiting for connection... Listening on "+str(socket.gethostbyname(socket.gethostname())+":"+ sys.argv[2]))
	while 1:
        	(clientsocket, address) = serversocket.accept()
		data = pickle.loads(clientsocket.recv(1024))
		Bank().bankhandle(data,sys.argv[1],clientsocket, int(sys.argv[3]))
		
			


