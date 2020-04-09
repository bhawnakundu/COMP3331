import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import org.omg.CORBA.portable.OutputStream;


public class cdht {
	private static String filename = "Simple_PDF.pdf";
	private static Node node;
	private static int mss;
	private static double dropProbability;
	private static final int UDP_BASE_PORT = 50000;
	private static final int TCP_FILE_BASE_PORT = 50000;
	private static final int UDP_FILE_BASE_PORT = 60000;
	private static final String PING_REQ_ACTION ="p";
	private static final String PNG_RES_ACTION ="r";
	private static final String FILE_REQ_ACTION ="fq";
	private static final String FILE_RES_ACTION ="fr";
	private static final String FILE_TRANSFER_ACTION ="ff";
	private static final String FILE_TRANSFER_RES_ACTION ="ffr";
	private static final String QUIT_ACTION="q";
	private static final String QUIT_RES_ACTION="qr";
	private static final String REQ_FIRST_SUC1_ACTION ="rfs";
	private static final String RES_FIRST_SUC1_ACTION ="rfsr";
	private static final String RES1_FIRST_SUC1_ACTION ="rfsr1";
	private static DatagramSocket socket= null;
	private static DatagramSocket fileTransferSocket = null;
	private static ServerSocket tcpServerSocket= null;
	private static int ackNum = 1;
	private static int squenceNum =1;
	private static String RESPONDING_LOG_FILE ="responding_log.txt";
	private static String REQUESTING_LOG_FILE ="requesting_log.txt";
	private static String EVENT_SND ="snd";
	private static String EVENT_RCV ="rcv";
	private static String EVENT_DROP ="drop";
	private static String EVENT_RTX ="rtx";
	private static final String REQUEST ="request ";
	private static final int NUM_NODE = 256;

	public static void main(String[] args) {
		if(args.length != 5) {
			System.out.println("Please provide sufficient arguments.");
			System.exit(1);
		}
		int name = Integer.parseInt(args[0]);
		int suc1 = Integer.parseInt(args[1]);
		int suc2 = Integer.parseInt(args[2]);
		mss = Integer.parseInt(args[3]);
		dropProbability = Double.parseDouble(args[4]);

		node = new Node(name);
		try {
			socket = new DatagramSocket(node.getName()+getUdpBasePort());
			// socket for file transfer
			fileTransferSocket = new DatagramSocket(node.getName()+getUDPFileBasePort());

			// ping two successors use UDP
			SendPackage sendPackage = new SendPackage(socket,node, suc1, suc2);
			sendPackage.start();

			// received the response from Ping Request
			ReceivePackage receivePackage = new ReceivePackage(socket, node, mss, suc1, suc2);
			receivePackage.start();

			// request file
			tcpServerSocket = new ServerSocket(TCP_FILE_BASE_PORT+node.getName());
			FileRequestServer fileRequestServer = new FileRequestServer(tcpServerSocket,node);
			fileRequestServer.start();

			FileRequestClient fileRequestClient = new FileRequestClient(node);
			fileRequestClient.start();

			ReceiveFilePackage receiveFilePackage = new ReceiveFilePackage(fileTransferSocket, mss,node);
			receiveFilePackage.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static int getUdpBasePort() {
		return UDP_BASE_PORT;
	}

	public static String getPingReqAction() {
		return PING_REQ_ACTION;
	}

	public static String getPngResAction() {
		return PNG_RES_ACTION;
	}

	public static String getFileReqAction() {
		return FILE_REQ_ACTION;
	}

	public static String getFileResAction() {
		return FILE_RES_ACTION;
	}
	
	public static String getReqFirstSuc1Action() {
		return REQ_FIRST_SUC1_ACTION;
	}
	
	public static String getResFirstSuc1Action() {
		return RES_FIRST_SUC1_ACTION;
	}
	
	public static String getRes1FirstSuc1Action() {
		return RES1_FIRST_SUC1_ACTION;
	}

	public static int getTcpFileBasePort() {
		return TCP_FILE_BASE_PORT;
	}

	public static int getUDPFileBasePort() {
		return UDP_FILE_BASE_PORT;
	}

	public static String getFileTransferAction() {
		return FILE_TRANSFER_ACTION;
	}

	public static String getFileTransferResponseAction() {
		return FILE_TRANSFER_RES_ACTION;
	}

	public static String getQuitResAction() {
		return QUIT_RES_ACTION;
	}

	public static void increaseAckNum() {
		ackNum++;
	}

	public static int getAckNum() {
		return ackNum;
	}

	public static int getSequenceNum() {
		return squenceNum;
	}

	public static void increaseSquenceNum() {
		squenceNum++;
	}

	public static int getMss() {
		return mss;
	}

	public static void write_log_file(String event, int sequenceNum, int bytes, int ackNum,int isRequest) {
		String filename = RESPONDING_LOG_FILE;
		if(isRequest == 1) {
			filename = REQUESTING_LOG_FILE;
		}
		try {
			FileWriter fw=new FileWriter(filename,true);
			fw.write(event+"\t"+(new Date()).toString()+"\t"+sequenceNum+"\t"+bytes+"\t"+ackNum+"\n");
			fw.close();
		}catch(Exception e) {
			System.out.println("Error writing into log file");
		}
	}

	public static String getEVENT_SND() {
		return EVENT_SND;
	}

	public static String getEVENT_DROP() {
		return EVENT_DROP;
	}

	public static String getEVENT_RCV() {
		return EVENT_RCV;
	}

	public static String getEVENT_RTX() {
		return EVENT_RTX;
	}

	public static String getQuitAction() {
		return QUIT_ACTION;
	}


	public static void sendmsgTo(DatagramSocket socket,int nodeToSend, String msg) {

		try {
			InetAddress IPAddress = InetAddress.getByName("localhost");

			DatagramPacket packet = new DatagramPacket(msg.getBytes(), msg.length(), IPAddress, nodeToSend+cdht.getUdpBasePort());

			socket.send(packet);

		} catch (Exception e) {
			if(socket!=null) socket.close();
			e.printStackTrace();
		}
	}

	public static void transferFileReqOrResTo(DatagramSocket socket,int nodeToSend, String msg, int flag,byte[] fileChunkbytes) {

		try {
	
			byte[] buffer = null;
			// sending the file chunk
			if(flag == 0) {

				msg += fileChunkbytes.length+";";

				buffer = new byte[msg.getBytes().length + fileChunkbytes.length];
				for(int i = 0; i < msg.getBytes().length; i++) {
					buffer[i] = msg.getBytes()[i];
				}
				for(int i = 0; i< fileChunkbytes.length; i++) {
					buffer[msg.getBytes().length+i] = fileChunkbytes[i];
				}
			}
			// sending the file end or response message
			else if(flag == 1) {
				buffer = msg.getBytes();
			}

			InetAddress IPAddress = InetAddress.getByName("localhost");

			DatagramPacket packet = new DatagramPacket(buffer, buffer.length, IPAddress, nodeToSend+cdht.getUDPFileBasePort());

			socket.send(packet);


		} catch (Exception e) {
			if(socket!=null) socket.close();
			e.printStackTrace();
		}
	}

	public static void sendMsgOverTCP(Socket socket, String msg) {
		try {
			DataOutputStream outToServer = new DataOutputStream(socket.getOutputStream());
			outToServer.writeBytes(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean hasTheFile(int num) {
		if( node.getName() == num) return true;
		else if(node.hasPredecessor1()) {
			if(node.getName() < node.getPredecessor1() && node.getPredecessor1() < num) {
				return true;
			}
			else if(node.getName() > num && num > node.getPredecessor1()) {
				return true;
			}
		}
		return false;
	}

	public static int findFileLocNode(int fileNum) {
		int num = hashFileNum(fileNum);
		if(node.getName() == num)
			return node.getName();
		else if(node.getName() < num && node.getName() < node.getPredecessor1()) {
			return node.getName();
		}
		else if(node.getName() > num && node.getPredecessor1() < num) {
			return node.getName();
		}
		return num;
	}

	private static int hashFileNum(int fileNum) {

		return fileNum % NUM_NODE;
	}

	public static void sendRequestResponseTCP(String action, int nodeNum) {
		try {
			String msg = node.messsage(action);
			Socket tcpClientSocketP1 = new Socket("localhost", cdht.getTcpFileBasePort()+nodeNum);
			sendMsgOverTCP(tcpClientSocketP1, msg);
			tcpClientSocketP1.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void fileReqeustActionTCP(int fileNum, int flag, int sourceNode) throws Exception{

		int num = findFileLocNode(fileNum);
		if(!hasTheFile(num)) {
			String msg = tcpFileReqMsg(fileNum, sourceNode);
			if(!node.hasSuccessor1() || !node.hasPredecessor1()) {
				System.out.println("Please wait, Looking for the successor and predecessor... and please try request the file again");
				return; 
			}
			Socket tcpClientSocket = new Socket("localhost", cdht.getTcpFileBasePort()+node.getSuccessor1());
			cdht.sendMsgOverTCP(tcpClientSocket, msg);
			tcpClientSocket.close();

			if(flag == 1) {
				System.out.println("File "+fileNum+" is not stored here.");
				System.out.println("File request message has been forwarded to my successor.");
			}
			else {
				System.out.println("File request message for "+fileNum+" has been sent to my successor.");
			}			
		}
		else {
			if(flag == 0) {
				System.out.println("You have already got the file. no need to send");
			}
			else {
				System.out.println("File "+fileNum +" is here.");

				SendFileResponseMsg(sourceNode, fileNum);

				SendFile(sourceNode);
			}
		}
	}

	private static void SendFile(int sourceNode) {
		System.out.println("We now start sending the file .........");

		// send file using UDP and getting the bytes from the file and cut them into blocks with MSS
		byte[] byteFromFile = getBytesFromFile(filename);
		// loop for each piece (construct the file msg block) , set the timer and send the packets
		int lenBytes = byteFromFile.length;
		int resend = 0;
		int dropped = 0;

		try {
			while(lenBytes > 0) {

				int preAckNum = ackNum;

				int bytesToSend = mss;
				if(lenBytes < mss) {
					bytesToSend = lenBytes;
				}

				// create random number and compare with drop probability
				double random_number = Math.random();
				if(random_number < dropProbability) {
					dropped = 1;
				}
				else {
					String msg = fileTransferMsg(squenceNum, ackNum);
					byte[] fileChunkbytes = getFileChunkInBytes(byteFromFile, squenceNum, mss, bytesToSend);
					transferFileReqOrResTo(fileTransferSocket, sourceNode, msg, 0, fileChunkbytes);
					dropped = 0;
				}

				// write to log file
				if(dropped ==1 ) {
					cdht.write_log_file(cdht.getEVENT_DROP(), squenceNum, bytesToSend, ackNum,0);
				}

				else if(resend == 0) {
					cdht.write_log_file(cdht.getEVENT_SND(), squenceNum, bytesToSend, ackNum,0);
				}
				else {
					cdht.write_log_file(cdht.getEVENT_RTX(), squenceNum, bytesToSend, ackNum,0);
					resend = 0;
				}
				Thread.sleep(1000);

				if(preAckNum == ackNum) {
					ackNum = preAckNum;
					squenceNum = ackNum;
					resend = 1;
				}
				else {
					lenBytes -= mss;
				}
			}

			String msg = fileTrasferEndMsg();
			transferFileReqOrResTo(fileTransferSocket, sourceNode, msg,1,null);

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("The file is sent.");
	}

	private static String fileTransferMsg (int sequence, int ack) {

		String str = getFileTransferAction()+";"+node.getName()+";"+ack+";"+sequence+";";
		return str;
	}

	private static byte[] getFileChunkInBytes(byte[] byteFromFile, int sequence,int preBytesSent, int bytesToSend) {

		byte[] result = new byte[bytesToSend];
		int result_index = 0;
		for (int i = (sequence-1)*preBytesSent; i < (sequence-1)*preBytesSent+bytesToSend; i++) {
			result[result_index++] = byteFromFile[i];
		}
		return result;
	}

	private static String fileTrasferEndMsg() {
		return getFileTransferAction()+";"+-1+";"+-1+";"+-1+";";
	}

	private static byte[] getBytesFromFile(String filePath) {
		FileInputStream fileInputStream = null;
		byte[] bytesArray = null;

		try {
			File file = new File(filePath);
			bytesArray = new byte[(int) file.length()];

			//read file into bytes[]
			fileInputStream = new FileInputStream(file);
			fileInputStream.read(bytesArray);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fileInputStream != null) {

				try {
					fileInputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bytesArray;
	}

	private static void SendFileResponseMsg(int sourceNode, int fileNum) throws Exception{
		String msg = tcpFileResMsg(fileNum);
		Socket tcpClientSocket = new Socket("localhost", cdht.getTcpFileBasePort()+sourceNode);
		cdht.sendMsgOverTCP(tcpClientSocket, msg);
		tcpClientSocket.close();
		System.out.println("A response message, destined for peer "+sourceNode +", has been sent.");
	}

	private static String tcpFileResMsg(int fileNum) {
		return cdht.getFileResAction()+";"+node.getName()+";"+fileNum;
	}

	private static String tcpFileReqMsg(int fileNum, int sourceNode) {
		return cdht.getFileReqAction()+";"+sourceNode+";"+fileNum;
	}
}

class FileRequestServer extends Thread{

	private ServerSocket serverSocket = null;
	private Node node;

	public FileRequestServer(ServerSocket ss, Node n) {
		this.serverSocket = ss;
		node = n;
	}

	@Override
	public void run() {
		try {
			while(true) {
				Socket fromClient = serverSocket.accept();
				BufferedReader in = new BufferedReader(new InputStreamReader(fromClient.getInputStream())); 
				String msgFromClient = null;
				while( (msgFromClient = in.readLine()) != null) {
					processFileRequestMsg(msgFromClient);
				}
				fromClient.close();
			}

		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	private void processFileRequestMsg(String msgFromClient) {
		try {
			String action = msgFromClient.split(";")[0];
			int sourceNode = Integer.parseInt(msgFromClient.split(";")[1]);

			if(action.equals(cdht.getFileReqAction())) {
				int fileNum = Integer.parseInt(msgFromClient.split(";")[2]);
				cdht.fileReqeustActionTCP(fileNum, 1, sourceNode);

			}
			else if(action.equals(cdht.getFileResAction())) {
				int fileNum = Integer.parseInt(msgFromClient.split(";")[2]);
				System.out.println("Received a response message from peer "+sourceNode+", which has the file "+fileNum+".");
				System.out.println("We now start receiving the file .........");

			}
			else if(action.equals(cdht.getQuitAction())) {
				System.out.println("Peer "+sourceNode+" will depart from the network.");
				int suc1FromMsg = Integer.parseInt(msgFromClient.split(";")[2]);
				int suc2FromMsg = Integer.parseInt(msgFromClient.split(";")[3]);
				int pre1FromMsg = Integer.parseInt(msgFromClient.split(";")[4]);
				int pre2FromMsg = Integer.parseInt(msgFromClient.split(";")[5]);

				if(node.getName() == pre1FromMsg) {
					node.setSuccessor1(suc1FromMsg);
					node.setSuccessor2(suc2FromMsg);
					System.out.println("My first successor is now peer "+suc1FromMsg+". My second successor is now peer "+suc2FromMsg+".");
				}
				else if(node.getName() == pre2FromMsg) {
					node.setSuccessor1(pre1FromMsg);
					node.setSuccessor2(suc1FromMsg);
					System.out.println("My first successor is now peer "+pre1FromMsg+". My second successor is now peer "+suc1FromMsg+".");
				}
				// send the message back to source 
				cdht.sendRequestResponseTCP(cdht.getQuitResAction(), sourceNode);
			}
			else if(action.equals(cdht.getQuitResAction())) {
				node.incDepartAckCount(sourceNode);
				if(node.getDepartAckCount() == 2) {
					System.exit(0);
				}
			}
			else if(action.equals(cdht.getReqFirstSuc1Action())) {
				// sending back the first successor
				cdht.sendRequestResponseTCP(cdht.getResFirstSuc1Action(), sourceNode);
			}
			else if(action.equals(cdht.getResFirstSuc1Action())) {
				int suc1FromMsg = Integer.parseInt(msgFromClient.split(";")[2]);
				
				node.setSuccessor2(suc1FromMsg);
				node.resetPingCountSuc2();
				System.out.println("My second successor is now peer "+node.getSuccessor2()+".");
				cdht.sendRequestResponseTCP(cdht.getRes1FirstSuc1Action(), node.getPredecessor1());
			}
			else if(action.equals(cdht.getRes1FirstSuc1Action())) {
				int suc1FromMsg = Integer.parseInt(msgFromClient.split(";")[2]);
				
				node.setSuccessor2(suc1FromMsg);
				node.resetPingCountSuc2();
				System.out.println("My second successor is now peer "+node.getSuccessor2()+".");
			}			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}

class FileRequestClient extends Thread{

	private final String REQUEST ="request ";
	private final String QUIT ="quit";
	private final int NUM_NODE = 256;
	private Node node;
	
	public FileRequestClient(Node n) {
		this.node = n;
	}

	@Override
	public void run() {
		try {

			// get input request from keyboard
			BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
			String inputMsg = "";
			int fileNum = -1;
			int valid = 0;
			int quit = 0;
			while(true) {
				do {
					valid = 0;
					quit = 0;
					fileNum = -1;
					inputMsg = inFromUser.readLine();

					if(inputMsg.startsWith(REQUEST)) {
						fileNum = checkFileName(inputMsg);
						if(fileNum == -1) {
							System.out.println("Invaild File Name, Please try again ....");
						}
						else {
							valid = 1;
						}
					}
					else if(inputMsg.equals(QUIT)) {
						quit = 1;
						valid = 1;
					}
					else {
						System.out.println("Invaild input request, Please try again ....");
					}
				}while(valid == 0);

				if(fileNum !=-1) {
					cdht.fileReqeustActionTCP(fileNum,0,node.getName());
				}
				else if(quit == 1) {
					cdht.sendRequestResponseTCP(cdht.getQuitAction(),node.getPredecessor1());
					cdht.sendRequestResponseTCP(cdht.getQuitAction(),node.getPredecessor2());
				}
			}
		}catch(Exception e) {
			System.out.println("Maybe Precessor not found yet");
		}
	}

	private int checkFileName(String inputMsg) {

		String filename = inputMsg.split(" ")[1];
		if(!filename.matches("[0-9]{4}")) return -1;
		return Integer.parseInt(filename);
	}
}

class SendPackage extends Thread{

	private Node node;
	private int suc1ToSend;
	private int suc2ToSend;
	private DatagramSocket socket;
	
	public SendPackage(DatagramSocket socket, Node n, int suc1TS, int suc2TS) {
		this.node = n;
		this.suc1ToSend = suc1TS;
		this.suc2ToSend = suc2TS;
		this.socket = socket;
		node.setSuccessor1(suc1ToSend);
		node.setSuccessor2(suc2ToSend);
	}

	@Override
	public void run() {
		while(true) {
			try {
				Thread.sleep(5000);

				//sending ping request message to two successors
				cdht.sendmsgTo(socket,node.getSuccessor1(), node.messsage(cdht.getPingReqAction()));
				node.incPingCountSuc1();

				cdht.sendmsgTo(socket,node.getSuccessor2(), node.messsage(cdht.getPingReqAction()));
				node.incPingCountSuc2();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}

class ReceiveFilePackage extends Thread{

	private DatagramSocket socket;
	private HashMap<Integer, byte[]> fileContentStringMap;
	private Node node;
	private int mss;

	public ReceiveFilePackage(DatagramSocket socket, int mss, Node node) {
		this.socket = socket;
		this.node = node;
		this.mss = mss;
		fileContentStringMap = new HashMap<>();
	}

	public void run() {

		try{

			byte[] buffReceived = new byte[mss*2];

			while(true) {
				DatagramPacket pktReceived = new DatagramPacket(buffReceived, buffReceived.length);
				socket.receive(new DatagramPacket(buffReceived, buffReceived.length));
				String msg = new String(pktReceived.getData(), 0, pktReceived.getLength());
				
				// process the received string
				processFileMsg(msg,buffReceived);
			}
		} catch (Exception e) {
			System.out.println("Error receiving the file message");
			e.printStackTrace();
		}
	}

	private void processFileMsg(String msg,byte[] buffReceived) {
		String action = msg.split(";")[0];
		int sourceNode = Integer.parseInt(msg.split(";")[1]);
		int ackNum = Integer.parseInt(msg.split(";")[2]);
		int sequenceNum = Integer.parseInt(msg.split(";")[3]);

		if(action.equalsIgnoreCase(cdht.getFileTransferAction())) {
			
			if(ackNum == -1) {
				// finish receiving file
				reconstructFile();
				System.out.println("The file is received.");
			}
			else {
				int bytesReceived = Integer.parseInt(msg.split(";")[4]);
				byte[] contentBytes = new byte[bytesReceived];
				String header = action+";"+sourceNode+";"+ackNum+";"+sequenceNum+";"+bytesReceived+";";
				int contentBytesIndex = 0;
				int headerLength = header.getBytes().length;
				for (int i = headerLength; i < headerLength + bytesReceived ; i++) {

					contentBytes[contentBytesIndex++] = buffReceived[i];
				}

				cdht.write_log_file(cdht.getEVENT_RCV(), sequenceNum, bytesReceived, ackNum,1);
				String responseMsg = fileAckResMsg(ackNum, sequenceNum);
				cdht.transferFileReqOrResTo(socket, sourceNode, responseMsg,1,null);

				fileContentStringMap.put(sequenceNum, contentBytes);	
			}
		}
		else if(action.equalsIgnoreCase(cdht.getFileTransferResponseAction())) {
			if(cdht.getAckNum() == ackNum && cdht.getSequenceNum() == sequenceNum) {
				cdht.increaseAckNum();
				cdht.increaseSquenceNum();
			}
		}
	}

	private String fileAckResMsg(int ackNum, int sequenceNum ) {
		return cdht.getFileTransferResponseAction()+";"+node.getName()+";"+ackNum+";"+sequenceNum+";";
	}

	private void reconstructFile() {
		
		try {
			File file = new File("received_file.pdf");
			FileOutputStream os = new FileOutputStream(file); 
			
			for(Entry<Integer, byte[]> entry : fileContentStringMap.entrySet()) {
				os.write(entry.getValue());
			}
			os.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

class ReceivePackage extends Thread{
	private Node node;
	private int mss;
	private DatagramSocket socket;
	private int suc1;
	private int suc2;
	private final int MAX_PING_COUNT = 4;

	public ReceivePackage(DatagramSocket socket,Node n, int m, int s1, int s2) {
		this.node = n;
		this.mss = m;
		this.socket = socket;
		this.suc1 = s1;
		this.suc2 = s2;
		node.setSuccessor1(suc1);
		node.setSuccessor2(suc2);
	}

	public void processMsg(String msg) {
		String action = msg.split(";")[0];
		int nodeFrom = Integer.parseInt(msg.split(";")[1]);
		// ping request message
		if(action.equalsIgnoreCase(cdht.getPingReqAction())) {
			printPingRequestMsg(nodeFrom);

			setNodePredesesoor(msg);

			// send the response
			cdht.sendmsgTo(socket, nodeFrom, node.messsage(cdht.getPngResAction()));
		}
		else if(action.equalsIgnoreCase(cdht.getPngResAction())) {
			printPingResponseMsg(nodeFrom);
			
			// declare successor die
			if(node.getPingCountSuc1() >= MAX_PING_COUNT) {
				System.out.println("Peer "+node.getSuccessor1()+" is no longer alive.");
				
				node.setSuccessor1(node.getSuccessor2());
				node.resetPingCountSuc1();
				
				System.out.println("My first successor is now peer "+node.getSuccessor1()+".");
				cdht.sendRequestResponseTCP(cdht.getReqFirstSuc1Action(), node.getSuccessor2());
			}
			
			if(node.getPingCountSuc2() >= MAX_PING_COUNT) {
				System.out.println("Peer "+node.getSuccessor2()+" is no longer alive.");
				
				System.out.println("My first successor is now peer "+node.getSuccessor1()+".");
				node.resetPingCountSuc2();
			}
			
			if(node.getSuccessor1() == nodeFrom) {
				node.decPingCountSuc1();
			}
			else if(node.getSuccessor2() == nodeFrom) {
				node.decPingCountSuc2();
			}
		}
	}

	private void setNodePredesesoor(String msg) {
		int nodeFrom = Integer.parseInt(msg.split(";")[1]);
		int m_suc1 = Integer.parseInt(msg.split(";")[2]);
		int m_suc2 = Integer.parseInt(msg.split(";")[3]);

		if(m_suc1 == node.getName()) { node.setPredecessor1(nodeFrom); }
		if(m_suc2 == node.getName()) {node.setPredecessor2(nodeFrom); }

	}

	public void printPingRequestMsg(int nodeNumber) {
		System.out.println("A ping request message was received from Peer "+nodeNumber+".");
	}
	public void printPingResponseMsg(int nodeNumber) {
		System.out.println("A ping response message was received from Peer "+nodeNumber+".");
	}

	public void run() {

		try{

			byte[] buffReceived = new byte[mss];

			while(true) {
				DatagramPacket pktReceived = new DatagramPacket(buffReceived, buffReceived.length);
				socket.receive(new DatagramPacket(buffReceived, buffReceived.length));
				String msg = new String(pktReceived.getData(), 0, pktReceived.getLength());

				// process the received string
				processMsg(msg);
			}
		} catch (Exception e) {
			System.out.println("Error receiving the message for node "+ node.getName());
		}
	}
}

class Node{
	private int name;
	private int predecessor1 = -1;
	private int predecessor2 = -1;
	private int successor1 =-1;
	private int successor2 =-1;
	private int pingCountSuc1 = 0;
	private int pingCountSuc2 = 0;
	private int departAckCount = 0;

	public Node(int name) {
		this.name = name;
	}

	public Node(int name, int suc1, int suc2) {
		this.name = name;
		this.successor1 = suc1;
		this.successor2 = suc2;
	}
	
	public void resetPingCountSuc1() {
		pingCountSuc1=0;
	}
	
	public void resetPingCountSuc2() {
		pingCountSuc2=0;
	}
	
	public void incDepartAckCount(int nodeNum) {
		if(nodeNum == predecessor1 || nodeNum==predecessor2) {
			departAckCount++;
		}
	}

	public int getPingCountSuc1() {
		return pingCountSuc1;
	}

	public int getPingCountSuc2() {
		return pingCountSuc2;
	}

	public int getDepartAckCount() {
		return departAckCount;
	}

	public int getName() {
		return name;
	}
	public void setName(int name) {
		this.name = name;
	}
	public int getPredecessor1() {
		return predecessor1;
	}
	public void setPredecessor1(int predecessor1) {
		this.predecessor1 = predecessor1;
	}
	public int getPredecessor2() {
		return predecessor2;
	}
	public void setPredecessor2(int predecessor2) {
		this.predecessor2 = predecessor2;
	}
	public int getSuccessor1() {
		return successor1;
	}
	public void setSuccessor1(int successor1) {
		this.successor1 = successor1;
	}
	public int getSuccessor2() {
		return successor2;
	}
	public void setSuccessor2(int successor2) {
		this.successor2 = successor2;
	}

	public void incPingCountSuc1() {
		pingCountSuc1++;
	}

	public void decPingCountSuc1() {
		pingCountSuc1--;
		if(pingCountSuc1<=0) {
			pingCountSuc1=0;
		}
	}

	public void incPingCountSuc2() {
		pingCountSuc2++;
	}
	public void decPingCountSuc2() {
		pingCountSuc2--;
		if(pingCountSuc2<=0) {
			pingCountSuc2=0;
		}
	}
	
	public boolean hasPredecessor1() {
		return predecessor1 != -1;
	}
	public boolean hasSuccessor1() {
		return successor1 != -1;
	}

	public String messsage(String action) {
		return action+";"+name+";"+successor1+";"+successor2+";"+predecessor1+";"+predecessor2;
	}
}
