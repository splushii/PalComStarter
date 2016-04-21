package se.lth.cs.palcom.updaterservice;

import java.io.IOException;
import java.util.Date;

import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.device.DeviceProperties;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.updaterservice.UpdaterService.UpdateState;

/**
 * Thread performing update stage two, updating PalComStarter. Typically started by a monitored device.
 * @author Christian Hernvall
 */
class UpdateStageTwoThread extends Thread {
	private UpdaterService us;
	private SocketListenerThread socketListener;
	private SocketSender socketSender;
	private static final int MAX_SECONDS_WAIT_FOR_DEVICE = 10;		
	private Process p = null;
	private String newPalComStarterCommand;
	private String currentPalComStarterCommand;
	private String newPalComStarterVersion;
	
	UpdateStageTwoThread(UpdaterService us, SocketListenerThread socketListener, SocketSender socketSender) {
		this.us = us;
		this.socketListener = socketListener;
		this.socketSender = socketSender;
	}
	
	@Override
	public void run() {
		us.log("Starting UpdateStageTwo Thread", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		// Setting some variables needed later in the process and in case of emergency abort			
		if(!stageTwoPreparations()) {
			us.log("Will not be able to perform stage two. Aborting update!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return;
		}

		try {
			stageTwo();
		} catch (Throwable t) {
			// If stage one could not be completed due to any reason, such as outOfMemoryError, we try to abort gracefully
			abortUpdateStageTwo("Update Stage One received unknown error/exception. Trying to abort gracefully. Message:\n" + t.getMessage());
		}	
	}
	
	private boolean stageTwoPreparations() {
		try {
			currentPalComStarterCommand = UpdaterService.toUTF8String(HostFileSystems.getGlobalRoot().getFile("startupscript").getContents());
		} catch (IOException e) {
			us.log("Could not access startup script.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		
		newPalComStarterVersion = socketListener.getMsg();
		// We need to know how to start the current PalcomStarter in case of error.
		newPalComStarterCommand = socketListener.getMsg();
		
		// Send kill to the current PalcomStarter
		socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_KILL, SocketSender.TRY_FOREVER);
		
		// Wait for PalComStarter to ACK our kill
		socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_KILL_ACK , SocketListenerThread.WAIT_FOREVER);
		return true;
	}
	
	private void stageTwo() {
		// Now we are in charge of the update process (so it is also our duty to abort if something goes wrong)
		
		// Start new PalcomStarter
		String[] arguments = newPalComStarterCommand.split(" ");
		String debug = "Going to start new version PalComStarter with:";
		for(String s: arguments)
			debug += " " + s;
		us.log(debug, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		ProcessBuilder pb = new ProcessBuilder(arguments);
		pb.inheritIO();
		try {
			us.log("Starting new PalComStarter...", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
			p = pb.start();
		} catch (IOException e) {
			abortUpdateStageTwo("Could not start new PalcomStarter. Aborting update!");
		}
		
		// check that communication via socket is working
		if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			abortUpdateStageTwo("Socket check timeout: Could not send msg to new device. Aborting update!");
		}
		String tmpMsg;
		tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
		if (tmpMsg == null || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
			abortUpdateStageTwo("Socket check timeout: No socket reply from new device. Aborting update!");
		}
		
		// check that palcomStarter can talk to update server
		if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_UPDATE_SERVER, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			abortUpdateStageTwo("Update Server check timeout: Could not send msg to new device. Aborting update!");
		}
		
		tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
		if (tmpMsg == null || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
			abortUpdateStageTwo("Update Server check timeout: No socket reply from new device. Aborting update!");
		}
		us.log("PalcomStarter can communicate.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		
		// PalcomStarter can communicate both by socket and to update server. Time to finish!
		
		if (!socketSender.sendMsg(newPalComStarterVersion, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			abortUpdateStageTwo("Finish stage two timeout: Could not send msg to new device. Aborting update!");
		}
		
		tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_STAGE_TWO, MAX_SECONDS_WAIT_FOR_DEVICE);
		if (tmpMsg == null || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
			abortUpdateStageTwo("Finish stage two timeout: Did not receive reply from new device. Aborting update!");
		}
		// make sure that we wont be disturbed and that the listening socket is closed before moving on
		us.setUpdateState(UpdateState.UPDATING_DONT_DISTURB);
		socketListener.closeSocket();
		if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_STAGE_TWO_ACK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			abortUpdateStageTwo("Socket timeout: Kill ack not received by new device. Aborting update!");
		}
		
		us.log("UpdateStageTwo Thread is done.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.setUpdateState(UpdateState.NONE);
	}
	
	private void abortUpdateStageTwo(String message) { // TODO
		us.log(message, Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
		if (p != null) {
			p.destroyForcibly();
		}
		// Set updateAborted variable so current PalComStarter knows that update failed
		try {
			if (us.monitoringProperties == null) {
				us.monitoringProperties = new DeviceProperties(new DeviceID("monitoring"), HostFileSystems.getGlobalRoot(), null, "Monitoring properties. Generated " + new Date());
			}
			String tmp = us.monitoringProperties.getProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED);
			if (tmp != null) {
				int nbrOfTimesAborted = Integer.valueOf(tmp);
				nbrOfTimesAborted++;
				us.monitoringProperties.setProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED, Integer.toString(nbrOfTimesAborted));
			} else {
				us.monitoringProperties.setProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED, "1");
			}
		} catch (IOException e) {
			us.log("Could not tell current PalComStarter that update was aborted.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
		}
		// Start current PalComStarter
		ProcessBuilder pb = new ProcessBuilder(currentPalComStarterCommand.split(" "));
		pb.inheritIO();
		try {
			pb.start();
		} catch (IOException e1) {
			e1.printStackTrace();
			us.log("MAJOR ERROR! Could not start current PalcomStarter. Hoping that the OS will start it later via the startup script...", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			System.exit(0);
		}
		us.stopDevice();
	}
}