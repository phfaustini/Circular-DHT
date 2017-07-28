/*
 * Assignment Comp3331 Computer Networks & Applications
 * 2015/1
 * University of New South Wales (UNSW), Australia
 * Author: Pedro Henrique Arruda Faustini - z5030903
 * Steps 1-4 working.
 * In order to compile, just type
 *		javac Cdht.java
 * More info in report.pdf
 */

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class Cdht {
	/*Members of a peer*/
	private int portSecond;
	private int portFirst;
	private int myPort;
	private int identifier;
	private static final int DELAY = 6000;  // milliseconds
	private int portFirstPredecessor;
	private int portSecondPredecessor;
	public ServerUDP serverUDP;
	public ServerTCP serverTCP;
	public ClientTCP clientTCP;
	
	/*Getters and setters*/
	public int getIdentifier() {
		return this.identifier;
	}
	public int getPortSecond() {
		return this.portSecond;
	}
	public int getPortFirst() {
		return this.portFirst;
	}

	/*Constructor*/
	public Cdht(int id, int f, int s) throws IOException{
		this.identifier = id;
		this.myPort = 5000 + id;
		this.portFirst = 5000 + f;
		this.portSecond = 5000 + s;
		this.portFirstPredecessor = -1;
		this.portSecondPredecessor = -1;
		
		this.serverUDP = new ServerUDP();
		this.serverTCP = new ServerTCP();
		this.clientTCP = new ClientTCP();
		
		/* Create threads. They're always checking whether something is arriving to this peer*/
		(new Thread(serverUDP)).start();
		(new Thread(serverTCP)).start();
		(new Thread(clientTCP)).start();
	}
		
	
	/*Inner classes*/
	private class ServerUDP implements Runnable{
		public DatagramSocket socket;
		private int aux;
		private boolean flag;
		/*Constructor*/
		public ServerUDP() throws SocketException{
			socket = new DatagramSocket(myPort); /*Create a datagram socket for receiving and sending UDP packets through the ports specified on command line.*/ 
			aux = -2;
			flag = false;
		}
		
		
		/*Auxiliary methods*/
		private boolean isRequestPing(DatagramPacket request){
			String sentence = new String( request.getData());
			if(sentence.startsWith("requestUDP"))
				return true;
			return false;
		}
		
		private void printData(DatagramPacket income) throws Exception { // Also stores first and second predecessors
			String sentence = new String(income.getData());
			int peer = income.getPort() - 5000;
			if (sentence.startsWith("requestUDP")){
				System.out.println("A ping request message was received from Peer "+ peer);
				if(portFirstPredecessor == -1)
					portFirstPredecessor = peer + 5000;
				else if(portSecondPredecessor == -1)
					portSecondPredecessor = peer + 5000;
				
				else if(income.getPort() != portFirstPredecessor && income.getPort() != portSecondPredecessor && flag == false){
					this.aux = income.getPort();
					this.flag = true;
				}
				else if(flag == true){
					flag = false;
					if(income.getPort() == portFirstPredecessor){
						portSecondPredecessor = income.getPort();
						portFirstPredecessor = this.aux;
					}
					else if(income.getPort() == portSecondPredecessor){
						portFirstPredecessor = income.getPort();
						portSecondPredecessor = this.aux;
					}
				}
				
				// Guarantee correct order of predecessors below
				if(portSecondPredecessor > -1 && portFirstPredecessor > -1){
					//Case1: normal situation (e.g. I am 4, predecessor1 is 1 and predecessor2 is 3, they must swap)
					if(portFirstPredecessor < portSecondPredecessor && myPort > portFirstPredecessor && myPort > portSecondPredecessor){
						int temp = portFirstPredecessor;
						portFirstPredecessor = portSecondPredecessor;
						portSecondPredecessor = temp;
					}
					//I am the smallest peer
					else if(myPort < portFirstPredecessor && myPort < portSecondPredecessor && portFirstPredecessor < portSecondPredecessor){
						int temp = portFirstPredecessor;
						portFirstPredecessor = portSecondPredecessor;
						portSecondPredecessor = temp;
					}
					// I am the second smallest peer, and my first predecessor is bigger than my second.
					else if(portFirstPredecessor > portSecondPredecessor && myPort > portSecondPredecessor && myPort < portFirstPredecessor){
						int temp = portFirstPredecessor;
						portFirstPredecessor = portSecondPredecessor;
						portSecondPredecessor = temp;
					}
				}
			}
			else if (sentence.startsWith("responseUDP"))
				System.out.println("A ping response message was received from Peer "+ peer);
		}
		
		/*UDP Server. The starting point for this code into the try/catch block was the PingServer.java file provided for the lab in week4. */
		@Override
		public void run() {							
				/* Create a datagram packet to hold incomming UDP packet.*/
				DatagramPacket request = new DatagramPacket(new byte[1024], 1024);
				/*Block until the host receives a UDP packet.*/
				while(true){
			        try {
			        	socket.receive(request);
			        	/*Print what has arrived. It can be either a request or response message, but printData handles that*/
			        	printData(request);
			        	//System.out.println("My predecessors are "+portFirstPredecessor+" "+portSecondPredecessor);
			        	/*Reply to the request saying this peer is up if the request is a request*/
					    if(isRequestPing(request)){
				        	InetAddress ClientHost = request.getAddress();
					        int ClientPort = request.getPort(); // Finds from where request came
					        String responseMessage = "responseUDP server up";
					        byte[] buf = responseMessage.getBytes();
					        DatagramPacket reply = new DatagramPacket(buf, buf.length, ClientHost, ClientPort);
					        socket.send(reply);
					    }
			        } catch (IOException e) {e.printStackTrace();} catch (Exception e) {e.printStackTrace();}
				}
		}			
	}
	
	/*The starting points for the classes ServerTCP and ClientTCP were taken from the TCPServer.java and TCPClient.java provided by this course.
	  Hence, the way this program handles TCP is very analogous, changing, of course, what the program does according to the message received.*/
	private class ServerTCP implements Runnable{
		public ServerSocket welcomeSocket;
		private int toBackPeerPort, selfDestroy;
		
		/*Constructor*/
		public ServerTCP () throws IOException{
			this.welcomeSocket = new ServerSocket(myPort);
			this.selfDestroy = -1;
		}

		/*Auxiliary methods*/
		private int hashFunction(String filename){
			return Integer.parseInt(filename) % 256;
		}		
		private boolean isStored(String filename, int predecessor){
			int key = hashFunction(filename);
			if( (key <= (myPort-5000) && key > (predecessor - 5000)) || ( myPort < predecessor && key > (predecessor - 5000)   ) || (key < (predecessor - 5000) && key <= (myPort - 5000) && myPort < predecessor ) )
				return true;
			return false;
		}		

		/*The TCP Server*/
		@Override
		public void run() {
			String clientSentence = "", filename = "";
			
			while(true){
				try {
					Socket connectionSocket = welcomeSocket.accept();
				    BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
				    clientSentence = inFromClient.readLine();
				    if(clientSentence.startsWith("request")){ // Someone is requesting a file to this peer
			            filename = clientSentence.split(" ")[1];
			            int predecessor = Integer.parseInt(clientSentence.split(" ")[3]);
			            if(isStored(filename, predecessor)){	// This peer has the file		
			            	int peer = Integer.parseInt(clientSentence.split(" ")[2]);
			            	peer -= 5000;
			            	System.out.println("File "+filename+" is here.\nA response message, destined for peer "+peer+", has been sent.");
			            	Socket toClient = new Socket("localhost", peer+5000);
			            	DataOutputStream outToClient = new DataOutputStream(toClient.getOutputStream());
						    outToClient.writeBytes("200 "+String.valueOf(myPort)+" "+filename); // 200 here means "I have the file"
						    toClient.close();
			            }
			            else{ // This peer does not have the file
			            	System.out.println("File "+filename+" is not stored here.\nFile request message has been forwarded to my successor.");
			            	Socket toClient = new Socket("localhost", portFirst);
			            	DataOutputStream outToClient = new DataOutputStream(toClient.getOutputStream());
			            	String forward = clientSentence.split(" ")[0]+" "+clientSentence.split(" ")[1]+" "+clientSentence.split(" ")[2]+" "+String.valueOf(myPort);
						    outToClient.writeBytes(forward);
						    toClient.close();
			            }
				    }
				    else if(clientSentence.startsWith("200")){ // This peer had requested a file and got it
				    	int peer = Integer.parseInt(clientSentence.split(" ")[1]);
				    	peer -= 5000;
				    	filename = clientSentence.split(" ")[2];
				    	System.out.println("Received a response message from peer "+peer+", which has the file "+filename+".");
				    }
				    else if(clientSentence.startsWith("update")){ // Server receives a gracefully quit request from a peer
				    	connectionSocket.close();
				    	
				    	portFirst = Integer.parseInt(clientSentence.split(" ")[1]);
				    	portSecond = Integer.parseInt(clientSentence.split(" ")[2]);
				    	this.toBackPeerPort = Integer.parseInt(clientSentence.split(" ")[3]);
				    	int peer = toBackPeerPort - 5000;
				    	int f = portFirst - 5000;
				    	int s = portSecond - 5000;
				    	System.out.println("Peer "+peer+" will depart the network.\nMy first successor is now peer  "+f+"\nMy second successor is now peer "+s);
				    	
				    	/*Reply, telling predecessors have been updated*/
				    	String toC = "300 "+String.valueOf(myPort);
				    	InetAddress serverIPAddress = InetAddress.getByName("localhost");
				    	Socket toClient = new Socket(serverIPAddress, toBackPeerPort);
				    	DataOutputStream outToClient = new DataOutputStream(toClient.getOutputStream());
				    	outToClient.writeBytes(toC);
					    toClient.close();
					}
				    else if(clientSentence.startsWith("300")){ // Check whether it is time to gracefully exit
				    	if(this.selfDestroy == -1)
				    		this.selfDestroy = Integer.parseInt(clientSentence.split(" ")[1]);
				    	else if(this.selfDestroy != Integer.parseInt(clientSentence.split(" ")[1])){
				    		System.exit(1);
				    	}
				    }
				    //connectionSocket.close();
				}catch (IOException e) {e.printStackTrace();}				
			}
		}
	}
	
	private class ClientTCP implements Runnable{		
		@Override
		public void run() {
			String sentence = "";
			while(true){ 
				try {
					BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
					sentence = inFromUser.readLine();
					String serverName = "localhost";
					InetAddress serverIPAddress = InetAddress.getByName(serverName);
					if(sentence.startsWith("request")){ // User is requesting a file to the P2P network
						Socket clientSocket = new Socket(serverIPAddress, portFirst);
						DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream()); 
						outToServer.writeBytes(sentence + " "+String.valueOf(myPort)+" "+String.valueOf(myPort));
						clientSocket.close();
						String file = sentence.split(" ")[1];
						System.out.println("File request message for "+file+" has been sent to my successor.");	
					}
					else if(sentence.startsWith("quit")){ // User decided to gracefully kill this peer				
						/*Send message to one peer*/
						Socket clientSocket = new Socket(serverIPAddress, portFirstPredecessor);
						DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream()); 
						String toS = "update "+String.valueOf(portFirst)+" "+String.valueOf(portSecond)+" "+String.valueOf(myPort);
						//System.out.println("update1 to "+portFirstPredecessor+ " "+String.valueOf(portFirst)+" "+String.valueOf(portSecond)+" "+String.valueOf(myPort));
						outToServer.writeBytes(toS);
						clientSocket.close();
						
						/*Send message to the other peer*/
						clientSocket = new Socket(serverIPAddress, portSecondPredecessor);
						outToServer = new DataOutputStream(clientSocket.getOutputStream());
						toS = "update "+String.valueOf(portFirstPredecessor)+" "+String.valueOf(portFirst)+" "+String.valueOf(myPort);
						//System.out.println("update2 to "+portSecondPredecessor +" "+String.valueOf(portFirstPredecessor)+" "+String.valueOf(portFirst)+" "+String.valueOf(myPort));
						outToServer.writeBytes(toS);
						clientSocket.close();
					}
				} catch (IOException e) {e.printStackTrace();}
				
			}
		}				
	}

	
	public static void main(String[] args) throws Exception {
		Cdht peer = new Cdht(Integer.parseInt(args[0]), Integer.parseInt(args[1]),Integer.parseInt(args[2]));
		
		while(true){			
			/*Peer pings its two successors to see whether they are alive*/
			InetAddress IPAddress = InetAddress.getByName("localhost");
			byte[] sendData = new byte[1024];
		    
			String sentence = "requestUDP "+peer.getIdentifier();
		    sendData = sentence.getBytes();		    
		    Thread.sleep((int) (DELAY));
			DatagramPacket msg = new DatagramPacket(sendData, sendData.length, IPAddress, peer.getPortFirst());
			peer.serverUDP.socket.send(msg);
			
			Thread.sleep((int) (DELAY));
			msg = new DatagramPacket(sendData, sendData.length, IPAddress, peer.getPortSecond());
			peer.serverUDP.socket.send(msg);
		}		
	}
}
