package se.lth.cs.palcom.updaterservice;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.Param;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.palcomstarter.PalComStarter;
import se.lth.cs.palcom.updaterservice.UpdaterService.UpdateState;
import se.lth.cs.palcom.util.configuration.DeviceList;

/**
 * Thread performing update stage one, updating monitored devices. Typically started by PalComStarter.
 * @author Christian Hernvall
 */

class UpdateStageOneThread extends Thread {
	private UpdaterService us;
	private MonitoringThread monitor;
	private SocketListenerThread socketListener;
	private SocketSender socketSender;
	private static final int MAX_SECONDS_WAIT_FOR_DATA = 15;
	private static final int MAX_SECONDS_WAIT_FOR_DEVICE = 10;
	private String pathToFS;
	private Writable conn;
	private String[] deviceTypes;
	private String[] newVersions;
	private LinkedList<PalComDeviceUpdateDescription> monitoredDeviceTypesToUpdate;
	private PalComDeviceUpdateDescription palComStarterUpdateDescription;
	private boolean performMonitoredDeviceUpdate;
	private boolean performPalComStarterUpdate;
	private boolean performMajorUpdate;

	public UpdateStageOneThread(UpdaterService us, MonitoringThread monitor, SocketListenerThread socketListener, SocketSender socketSender, Writable conn, String[] deviceTypes, String[] deviceTypeNewVersions) {
		this.us = us;
		this.monitor = monitor;
		this.socketListener = socketListener;
		this.socketSender = socketSender;
		this.conn = conn;
		this.deviceTypes = deviceTypes;
		this.newVersions = deviceTypeNewVersions;
		monitoredDeviceTypesToUpdate = new LinkedList<PalComDeviceUpdateDescription>();
	}

	@Override
	public void run() {
		us.log("UpdateStageOne Thread started.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);		
		// Preparing and setting some variables needed later in the process and in case of emergency abort
		if (!stageOnePreparations()) {
			us.log("No update will be performed.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			us.setUpdateState(UpdateState.NONE);
			return;
		}
		
		try {
			stageOne();
		} catch (Throwable t) {
			// If stage one could not be completed due to any reason, such as outOfMemoryError, we try to abort gracefully
			abortUpdateStageOne("Update Stage One received unknown error/exception. Trying to abort gracefully. Message:\n");
			t.printStackTrace();
		}
	}
	
	private boolean stageOnePreparations() {
		// Check if we recently tried to update and failed
		if (us.updateAborted > 0 && us.updateAbortedDelay > System.currentTimeMillis()) {
			us.log("Recently tried to update (" + us.updateAborted +" times) and failed. Updating disabled for " + (us.updateAbortedDelay - System.currentTimeMillis())/1000 + "s.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			return false;
		}
		pathToFS = HostFileSystems.getUnixStylePathToFilesystemRoot().replace("/PalcomFilesystem","");
		// Check what type of updates we are to perform
		performMonitoredDeviceUpdate = false;
		performPalComStarterUpdate = false;
		performMajorUpdate = false; // TODO check for this when the other stuff is working
		for (int i = 0; i < deviceTypes.length; ++i) {
			String deviceType = deviceTypes[i];
			String newVersion = newVersions[i];
			if (monitor.monitorsDeviceType(deviceType) || deviceType.equals(UpdaterService.PALCOMSTARTER_DEVICE_TYPE)) {
				String currentVersion;
				currentVersion = us.monitoringProperties.getProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, deviceType);
				PalComDeviceUpdateDescription pdu = new PalComDeviceUpdateDescription(deviceType, currentVersion, newVersion);
				if (pdu.isIncompatible()) {
					us.log("The version schemes are incompatible. Local version of " + deviceType + " (" + currentVersion + ") is not comparable with UpdateServer's version (" + newVersion + ").", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
					us.log("Will not update " + deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
					continue;
				}
				if (pdu.isIdentical()) {
					us.log("Our current version of " + deviceType + " (" + currentVersion + ") does not differ from UpdateServer's version (" + newVersion + ").", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					us.log("Will not update " + deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					continue;
				}
				if (!pdu.isUpgrade()) {
					us.log("Our current version of " + deviceType + " (" + currentVersion + ") is greater than the UpdateServer's version (" + newVersion + ").", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					us.log("Will not update " + deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					continue;
				}
				
				// Now we are interested in this update. It is time to fetch the update content
				us.setUpdateState(UpdateState.UPDATING_WAITING_FOR_JAR);
				Command updateContentRequest = us.getCommand(UpdaterService.COMMAND_OUT_UPDATE_CONTENT_REQUEST);
				updateContentRequest.findParam(UpdaterService.PARAM_DEVICE_TYPE).setData(deviceType.getBytes());
				updateContentRequest.findParam(UpdaterService.PARAM_VERSION).setData(newVersion.getBytes());
				us.sendPalComMessage(conn, updateContentRequest);
				us.log("Waiting for update data from Update Server...", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				Command command = us.getCommandFromBuffer(UpdaterService.COMMAND_IN_UPDATE_DATA, MAX_SECONDS_WAIT_FOR_DATA);
				if (command == null || command.getID().equals(UpdaterService.COMMAND_IN_ABORT_UPDATE)){
					us.log("Timeout when waiting for update data to " + deviceType + " " + newVersion + ". Will not update " + deviceType + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					continue;
				}
				
				// Extract version info and jar content.
				Param pDeviceType = command.findParam(UpdaterService.PARAM_DEVICE_TYPE);
				String receivedDeviceType = UpdaterService.toUTF8String(pDeviceType.getData());
				Param pVersion = command.findParam(UpdaterService.PARAM_VERSION);
				String receivedVersion = UpdaterService.toUTF8String(pVersion.getData());
				us.log("Received update to " + receivedDeviceType + " with version " + receivedVersion, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				
				// Check to see if it matches what we requested
				if (!deviceType.equals(receivedDeviceType) || !newVersion.equals(receivedVersion)) {
					us.log("Expected device type or version did not match received "
							+ "device type or version from UpdateServer. Will not update " + deviceType + "!\n"
							+ "Expected: " + deviceType + " version " + newVersion + "\n"
							+ "Received: " + receivedDeviceType + " version " + receivedVersion, Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					continue;
				}
				
				// Save new executable
				Param pContent = command.findParam(UpdaterService.PARAM_UPDATE_CONTENT);
				byte[] content = pContent.getData();
				String newExecPath;
				try {
					newExecPath = DeviceList.getConfFolder(deviceType).getNativeURL().replace("file:", "")
							+ "/" + deviceType + "-" + newVersion + ".jar";
				} catch (IOException e1) {
					us.log("Could not access global configuration folder for " + deviceType + ". Will not update " + deviceType + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					continue;
				}
				pdu.pathToExec = newExecPath;
				if (!us.saveJar(content, newExecPath)) {
					us.log("Could not save jar: " + newExecPath + ". Will not update " + deviceType + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					new File(newExecPath).delete();
					continue;
				}
				if (pdu.isProtocolBreaking()) {
					monitor.setNewMajorVersion(deviceType, pdu.newMajor);
				}
				us.log("New executable for " + deviceType + " saved to: " + newExecPath, Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				
				us.log("We will be updating " + deviceType + " from version (" + currentVersion + ") to version (" + newVersion + ").", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				if (deviceType.equals(UpdaterService.PALCOMSTARTER_DEVICE_TYPE)) {
					performPalComStarterUpdate = true;
					palComStarterUpdateDescription = pdu;
				} else {
					performMonitoredDeviceUpdate = true;
					monitoredDeviceTypesToUpdate.add(pdu);
				}
			} else {
				us.log("Update's device type " + deviceType + " does not match any of our monitored devices", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			}
		}
		
		if (monitor.readyForMajorUpdate()) {
			performMajorUpdate = true;
			us.log("All devices are ready for a protocol breaking update.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		} else {
			us.log("All devices are not ready for a protocol breaking update.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			if (palComStarterUpdateDescription.isProtocolBreaking()) {
				us.log("Will not perform a protocol breaking update of PalComStarter.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				palComStarterUpdateDescription = null;
				performPalComStarterUpdate = false;
			}
			for (int i = 0; i < monitoredDeviceTypesToUpdate.size();) {
				PalComDeviceUpdateDescription pdu = monitoredDeviceTypesToUpdate.get(i);
				if (pdu.isProtocolBreaking()) {
					us.log("Will not perform a protocol breaking update of " + pdu.deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					monitoredDeviceTypesToUpdate.remove(i);
					continue;
				}
				++i;
			}
			if (monitoredDeviceTypesToUpdate.isEmpty())
				performMonitoredDeviceUpdate = false;
		}
		
		if (performMonitoredDeviceUpdate || performPalComStarterUpdate || performMajorUpdate)
			return true;
		else
			return false;
	}
		
	private void stageOne() {
		us.setUpdateState(UpdateState.UPDATING_KILLING_CURRENT);

		// Disable monitoring. (Otherwise the current devices would be started again by the monitoring thread),
		// and the monitoring thread will get in the way (for example using the socket threads).
		monitor.disable();
		
		// Perform monitored device update
		MonitoredDevice d = null;
		for (PalComDeviceUpdateDescription ud: monitoredDeviceTypesToUpdate) {
			List<MonitoredDevice> md = monitor.getMonitoredDevicesOfType(ud.deviceType);
			for(MonitoredDevice monitoredDevice: md) {
				d = monitoredDevice;
				monitor.killMonitoredDevice(d, true);
				// Check that socket is working
				if(!monitor.startNewVersionMonitoredDevice(d, ud.newVersion)) {
					us.log("Could not start " + d.deviceType + " " + d.deviceID + " " + ud.newVersion + ". Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					if (performMajorUpdate)
						abortUpdateStageOne("");
					else
						continue;
				}
				if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET, MAX_SECONDS_WAIT_FOR_DEVICE)) {
					us.log("Socket check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					monitor.killMonitoredDevice(d, false);
					if (performMajorUpdate)
						abortUpdateStageOne("");
					else
						continue;
				}
				
				String tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_SOCKET_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
				if (tmpMsg == null || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
					us.log("Socket check timeout: No socket reply from new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					monitor.killMonitoredDevice(d, false);
					if (performMajorUpdate)
						abortUpdateStageOne("");
					else
						continue;
				}
				
				// Check that device can talk to update server
				us.setUpdateState(UpdateState.UPDATING_FALLBACK_TIMER_CHECK_UPDATE_SERVER);
				if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_UPDATE_SERVER, MAX_SECONDS_WAIT_FOR_DEVICE)) {
					us.log("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					monitor.killMonitoredDevice(d, false);
					if (performMajorUpdate)
						abortUpdateStageOne("");
					else
						continue;
				}
				if (!socketSender.sendMsg(us.updateServerDeviceID, MAX_SECONDS_WAIT_FOR_DEVICE)) {
					us.log("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					monitor.killMonitoredDevice(d, false);
					if (performMajorUpdate)
						abortUpdateStageOne("");
					else
						continue;
				}
				tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_CHECK_UPDATE_SERVER_CONFIRM, MAX_SECONDS_WAIT_FOR_DEVICE);
				if (tmpMsg == null  || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
					us.log("Update Server check timeout: No socket reply from new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
					monitor.killMonitoredDevice(d, false);
					if (performMajorUpdate)
						abortUpdateStageOne("");
					else
						continue;
				}
				// Do this for all but the last monitored device. Hold on to the last one a bit longer.
				if (monitoredDeviceTypesToUpdate.indexOf(ud) != (monitoredDeviceTypesToUpdate.size() - 1) || md.indexOf(d) != (md.size() - 1)) {
					if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
						us.log("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
						monitor.killMonitoredDevice(d, false);
						if (performMajorUpdate)
							abortUpdateStageOne("");
						else
							continue;
					}
					tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, MAX_SECONDS_WAIT_FOR_DEVICE);
					if (tmpMsg == null  || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
						us.log("Update Server check timeout: No socket reply from new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
						monitor.killMonitoredDevice(d, false);
						if (performMajorUpdate)
							abortUpdateStageOne("");
						else
							continue;
					}						
				}
			}
			// All monitored devices with deviceType are working, so we update the current version of deviceType
			us.log("Successfully updated " + ud.deviceType + " to version " + ud.newVersion, Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			monitor.setCurrentDeviceTypeVersion(ud.deviceType, ud.newVersion);
			us.monitoringProperties.setProperty(UpdaterService.NAMESPACE_UPDATERSERVICE_DEVICE_TYPE_VERSION, ud.deviceType, ud.newVersion);
		}

		// If we will not update PalComStarter, we can let the last monitored device go. 
		// Otherwise, we need it later for stage two.
		if (performMonitoredDeviceUpdate && !performPalComStarterUpdate) {
			if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				us.log("Update Server check timeout: Could not send msg to new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
				monitor.killMonitoredDevice(d, false);
				abortUpdateStageOne("");
			}
			String tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_FINISH_DEVICE_STARTUP_CHECK_ACK, MAX_SECONDS_WAIT_FOR_DEVICE);
			if (tmpMsg == null  || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
				us.log("Update Server check timeout: No socket reply from new device. Will not update " + d.deviceID + "!", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
				monitor.killMonitoredDevice(d, false);
				abortUpdateStageOne("");
			}
			us.log("Updating process finished.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			monitor.enable();
			us.setUpdateState(UpdateState.NONE);
			return;
		}
		
		us.log("We are about to update PalComStarter. Time for update stage two.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
		
		if (!performMonitoredDeviceUpdate) { // If we did not perform update on some monitored device, 
											 // we need to initiate stage two by palcom messages.
			// It is always possible to communicate with PalCom messages in this case, because monitored 
			// devices need to be updated in the event of a major update.
			d = monitor.initiateStageTwo();
			if (d == null) {
				us.log("Could not get hold of a monitored device to initiate stage two with. Will not update " + UpdaterService.PALCOMSTARTER_DEVICE_TYPE + ".", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
				abortUpdateStageOne("");
			}
		} else { // ... else, we initiate stage two by Update Protocol messages
			if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_STAGE_TWO, MAX_SECONDS_WAIT_FOR_DEVICE)) {
				us.log("Socket timeout: Could not send msg to new device. Will not update " + UpdaterService.PALCOMSTARTER_DEVICE_TYPE + ".", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
				abortUpdateStageOne("");
			}
		}
		// Send version of new PalComStarter (used to update startup script in stage three)
		if (!socketSender.sendMsg(palComStarterUpdateDescription.newVersion, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			us.log("Socket timeout: Could not send msg to new device. Will not update " + UpdaterService.PALCOMSTARTER_DEVICE_TYPE + ".", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			abortUpdateStageOne("");
		}
		// Sending the start command for the new version of PalComStarter
		String myJarPath = palComStarterUpdateDescription.pathToExec;
		String myDeviceID = us.getDevice().getDeviceID().getID();
		String newStartCmd = "java -jar " + myJarPath + " -x " + myDeviceID + " -f " + pathToFS;
		// Add the flag telling PalComStarter to continue with stage three when starting:
		newStartCmd += " -" + PalComStarter.COM_CONTINUE_UPDATE_STAGE_THREE;
		if (!socketSender.sendMsg(newStartCmd, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			us.log("Socket timeout: Could not send msg to new device. Will not update " + UpdaterService.PALCOMSTARTER_DEVICE_TYPE + ".", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			abortUpdateStageOne("");
		}
		
		/** TODO we should send Process objects so that the new version of PalComStarter can directly 
		 * kill a process. Sadly this is not possible. Process not serializable = not possibru.
		 * 
		 * Instead, we should turn around the updating process. First, PalComStarter is updated, then 
		 * monitored devices. This way, the new PalComStarter will have Process-objects to kill its 
		 * monitored devices.
		 */
		
		// Wait for killing blow...
		String tmpMsg = socketListener.waitForMsg(UpdaterService.UPDATE_PROTOCOL_KILL, MAX_SECONDS_WAIT_FOR_DEVICE);
		if (tmpMsg == null || tmpMsg.equals(UpdaterService.UPDATE_PROTOCOL_ABORT)) {
			abortUpdateStageOne("Socket timeout: No kill reply from new device. Aborting update!");
		}
		// Make sure that we wont be disturbed and that the listening socket is closed before moving on
		us.setUpdateState(UpdateState.UPDATING_DONT_DISTURB);
		socketListener.closeSocket();
		if (!socketSender.sendMsg(UpdaterService.UPDATE_PROTOCOL_KILL_ACK, MAX_SECONDS_WAIT_FOR_DEVICE)) {
			abortUpdateStageOne("Socket timeout: Kill ack not received by new device. Aborting update!");
		}
		us.log("UpdateStageOne Thread done. Thread killed.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		us.stopDevice();
	}
	
	private void abortUpdateStageOne(String message) {
		us.log(message, Logger.CMP_SERVICE, Logger.LEVEL_ERROR);

		// If we were performing a major update, we need to reset everything.
		if (performMajorUpdate) {
			monitor.killAllNewVersionMonitoredDevices();
			monitor.restartAllMonitoredDevices();
//			for (String newJarPath: newJarPaths) {
				// remove the jar
//			}
		} else {
			// remove the aborted jar
		}
		
		// Reopen socket if it is closed
		socketListener.reopenSocket();			
		
		us.updateAborted++;
		
		us.setUpdateState(UpdateState.NONE);
		monitor.enable();
	}
}