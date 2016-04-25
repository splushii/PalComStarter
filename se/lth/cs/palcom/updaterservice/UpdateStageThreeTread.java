package se.lth.cs.palcom.updaterservice;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.updatedistributionservice.UpdateDistributionService;
import se.lth.cs.palcom.util.configuration.DeviceList;

/**
 * Thread performing update stage three, finishing update of PalComStarter and updating startup script. 
 * Typically started by PalComStarter.
 * @author Christian Hernvall
 */

class UpdateStageThreeThread extends Thread {
	private UpdaterService us;
	private SocketListenerThread socketListener;
	private SocketSender socketSender;
	private String currentPalcomStarterCommand;
	private Writable writableConnToUpdateServer;
	private String deviceID;
	private String startupScriptURL;
	private String newStartupCommand;
	private File startupScriptBackup;
	private File startupScript;
	private String newVersion;

	UpdateStageThreeThread(UpdaterService us, SocketListenerThread socketListener, SocketSender socketSender) {
		this.us = us;
		this.socketListener = socketListener;
		this.socketSender = socketSender;
	}
	
	@Override
	public void run() {
		us.log("UpdateStageThree Thread started.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		// Setting some variables needed later in the process and in case of emergency abort			
		if (!stageThreePreparations()) {
			us.log("Will not be able to perform stage three. Aborting update!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			
			return;
		}

		try {
			stageThree();
		} catch (Throwable t) {
			// If stage one could not be completed due to any reason, such as outOfMemoryError, we try to abort gracefully
			abortUpdateStageThree("Update Stage One received unknown error/exception. Trying to abort gracefully. Message:\n" + t.getMessage());
		}	
	}
	
	private boolean stageThreePreparations() {			
		deviceID = us.getDevice().getDeviceID().getID();

		socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET, SocketListenerThread.WAIT_FOREVER);
		socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, SocketSender.TRY_FOREVER);
		socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_UPDATE_SERVER, SocketListenerThread.WAIT_FOREVER);
		if(us.updateServerDeviceID == null) { // updateServerDeviceID is set in UpdaterService's constructor
			us.log("Could not find UpdateServer's deviceID in monitoring.properties: " + UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL + "@" + UpdaterService.KEY_UPDATE_SERVER_DEVICE_ID, Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			us.log("Exiting.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			us.stopDevice();
		}
		
		// now we try to communicate with the updateServer
		writableConnToUpdateServer = us.getWritableConnectionToService(new DeviceID(us.updateServerDeviceID), UpdateDistributionService.SERVICE_NAME, -1);
		
		// request response from update server in order to test Palcom tunnel/communication
		Command confirmRequestCmd = us.getCommand(UpdaterService.COMMAND_OUT_CHECK_UPDATE_SERVER);
		us.log("Sending confirmmation request to update server", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.sendPalComMessage(writableConnToUpdateServer, confirmRequestCmd);
		
		us.log("Waiting for confirmation from update server.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.getCommandFromBuffer(UpdaterService.COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM);

		us.log("Got response from update server!", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, SocketSender.TRY_FOREVER);
		
		newVersion = socketListener.getMsg();
		
		// Make sure that we can write to the startupscript and to make a backup
		try {
			startupScriptURL = HostFileSystems.getGlobalRoot().getFile("startupscript").getNativeURL().replace("file:", "");
		} catch (IOException e) {
			us.log("Could not get startupScript URL.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		startupScript = new File(startupScriptURL);
		if (!startupScript.isFile()) {
			us.log("StartupScript is not a file.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		if (!startupScript.canRead()) {
			us.log("StartupScript is not readable.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		if (!startupScript.canWrite()) {
			us.log("StartupScript is not writable.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		
		// Prepare new startup script content
		String pathToFS = HostFileSystems.getUnixStylePathToFilesystemRoot().replace("/PalcomFilesystem", "");
		String pathToExec;
		try {
			pathToExec = DeviceList.getConfFolder(UpdaterService.PALCOMSTARTER_DEVICE_TYPE).getNativeURL().replace("file:", "");
		} catch (IOException e) {
			us.log("Could not get path to executable.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		pathToExec += UpdaterService.PALCOMSTARTER_DEVICE_TYPE + "-" + newVersion + ".jar";
		newStartupCommand = "java -jar " + pathToExec + " -x " + deviceID + " -f " + pathToFS;
		
		// Creating backup
		startupScriptBackup = new File(startupScriptURL + ".bak");
		if (startupScriptBackup.exists())
			startupScriptBackup.delete();
		try {
			Files.copy(startupScript.toPath(), startupScriptBackup.toPath());
		} catch (IOException e1) {
			us.log("Could not create backup of startup script file.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			return false;
		}
		
		// finish stage two
		socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_STAGE_TWO, SocketListenerThread.WAIT_FOREVER);
		
		// Wait for other device to ACK our finish
		socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_STAGE_TWO_ACK, SocketListenerThread.WAIT_FOREVER);
		return true;
	}
	
	private void stageThree() {
		// Now we are in charge of the update process
		us.log("Updating host's startup script", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		
		// Write new content to startup script
		FileWriter fw = null;
		try {
			fw = new FileWriter(startupScript, false);
		} catch (IOException e) {
			// Can not happen! Already checked in preparations!
			e.printStackTrace();
			fw = null;
			// Abort if this happens, becase something must be very wrong
			abortUpdateStageThree("StartupScript file cannot be opened for unknown reason. Aborting update!");
		}
		try {
			fw.write(newStartupCommand);
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// Restore startupScript backup
			startupScript.delete();
			if (!startupScriptBackup.renameTo(startupScript)) {							
				us.log("MAJOR ERROR! Could not restore startupScript backup!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			}
			abortUpdateStageThree("Error while updating startup script. Aborting update!");
		}
		// Remove startupScript backup
		startupScriptBackup.delete();
		
//		}
		// Remove update aborted counter so it won't effect next updating process
		us.monitoringProperties.removeProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED);
	
		// TODO Delete old version executables?
		
		us.monitoringProperties.setProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, UpdaterService.PALCOMSTARTER_DEVICE_TYPE, newVersion);
		us.log("Updating done", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
	}
	
	private void abortUpdateStageThree(String message) { //TODO
		us.log(message, Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
		// Set updateAborted variable so current PalComStarter knows that update failed
		String tmp = us.monitoringProperties.getProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED);
		if (tmp != null) {
			int nbrOfTimesAborted = Integer.valueOf(tmp);
			nbrOfTimesAborted++;
			us.monitoringProperties.setProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED, Integer.toString(nbrOfTimesAborted));
		} else {
			us.monitoringProperties.setProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL, UpdaterService.KEY_UPDATE_ABORTED, "1");
		}
		// Start current PalComStarter
		ProcessBuilder pb = new ProcessBuilder(currentPalcomStarterCommand.split(" "));
		pb.inheritIO();
		try {
			pb.start();
		} catch (IOException e1) {
			e1.printStackTrace();
			us.log("MAJOR ERROR! Could not start current PalcomStarter. Hoping that the OS will start it later via the startup script...", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			System.exit(0);
		}
		// TODO Tell JVM to delete new filesystem when it shuts down
//		new File(basePath).deleteOnExit();
//		deleteFilesystem(basePath, true);
//		stopDevice();
	}
}