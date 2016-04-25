package se.lth.cs.palcom.updaterservice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import se.lth.cs.palcom.logging.Logger;

/**
 * A helper class running as a thread used to receive and buffer Update Protocols commands over TCP.
 * Ugly, but does its job.
 * @author Christian Hernvall
 *
 */
class SocketListenerThread extends Thread {
	static final int WAIT_FOREVER = -1;
	private int listeningPort;
	private LinkedBlockingQueue<String> msgBox;
	private ServerSocket ss;
	private Semaphore wakeLock = new Semaphore(0);
	private boolean halt = false;
	private UpdaterService us;
	
	SocketListenerThread(UpdaterService us, int port) {
		this.listeningPort = port;
		this.us = us;
		msgBox = new LinkedBlockingQueue<String>();
	}
	
	void stopThread() {
		if (!halt) {
			halt = true;
			if(ss.isClosed()) {
				wakeLock.release();
			} else {
				closeSocket();				
			}			
		}
	}
	
	String getMsg() {
		try {
			return msgBox.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	String waitForMsg(String msgID, int waitInSeconds) {
		String msg = null;
		long stopTimeMillis = System.currentTimeMillis() + waitInSeconds*1000;
		while(true) {
			try {
				if (waitInSeconds == WAIT_FOREVER) {
					msg = msgBox.take();
				} else {
					long currentTimeMillis = System.currentTimeMillis();
					if (stopTimeMillis < currentTimeMillis) {
						return msg;
					}
					msg = msgBox.poll(stopTimeMillis-currentTimeMillis, TimeUnit.MILLISECONDS);					
				}
				if (msg != null && msg.equals(msgID)) {
					return msg;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}				
		}
	}
	
	private void addMsg(String msg) {
		try {
			msgBox.put(msg);
		} catch (InterruptedException e) {
			us.log("Got interrupted when trying to add msg \"" + msg + "\" to SocketListener's msgBox", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			e.printStackTrace();
		}
	}
	
	void closeSocket() {
		try {
			ss.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	boolean isSocketClosed() {
		return ss.isClosed();
	}
	
	void reopenSocket() {
		wakeLock.release();
	}

	@Override
	public void run() {
		us.log("SocketListener Thread started. Socket listening on port " + listeningPort, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		try {
			ss = new ServerSocket(listeningPort);
		} catch (IOException e1) {
			e1.printStackTrace();
			us.log("SocketListener Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			return;
		}
		Socket sock;
		while (true) {
			try {
				sock = ss.accept();
//				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
				String line = null;
				while ((line = in.readLine()) != null) {
					us.log("Socket message received \"" + line + "\"", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					addMsg(line);
				}
			} catch (SocketException e) {
				if (halt) {
					us.log("SocketListener Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					return;
				}
				us.log("SocketException (" + e.getMessage() + ") in SocketListener. Waiting until awoken.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				try {
					ss.close();
				} catch (IOException e2) {
					us.log("Could not close socket in SocketListener.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				}
				wakeLock.acquireUninterruptibly();
				if (halt) {
					us.log("SocketListener Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
					return;
				}
				try {
					ss = new ServerSocket(listeningPort);
				} catch (IOException e2) {
					e2.printStackTrace();
					us.log("Could not open SocketListener again. Returning", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();					
			}
		}
		us.log("SocketListener Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
	}
}