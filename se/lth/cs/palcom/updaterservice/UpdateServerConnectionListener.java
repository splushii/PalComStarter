package se.lth.cs.palcom.updaterservice;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.communication.connection.Connection;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.discovery.Resource;
import se.lth.cs.palcom.discovery.ResourceListener;
import se.lth.cs.palcom.discovery.proxy.PalcomDevice;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.updatedistributionservice.UpdateDistributionService;

/**
 * Keeps track of the connection to the UpdateServer.
 * @author Christian Hernvall
 */
class UpdateServerConnectionListener implements ResourceListener {
	private UpdaterService us;
	private MonitoringThread monitor;
	private static final int MAX_SECONDS_WAIT_FOR_CONNECTION = 5;
	private Writable writableConnToUpdateServer = null;
	private DeviceID updateServerDID;
	private PalcomDevice updateServerDevice;
	
	UpdateServerConnectionListener(UpdaterService us, MonitoringThread monitor) {
		this.us = us;
		this.monitor = monitor;
		if (us.updateServerDeviceID == null) {
			us.log("No Device ID to UpdateServer in configuration: " + UpdaterService.NAMESPACE_UPDATERSERVICE_GENERAL + "@" + UpdaterService.KEY_UPDATE_SERVER_DEVICE_ID, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return;
		}
		updateServerDID = new DeviceID(us.updateServerDeviceID);
		updateServerDevice = us.getDevice().getDiscoveryManager().getDevice(updateServerDID);
	}
	
	void addUpdateServerListener() {
		updateServerDevice.addListener(this);
	}
	
	void removeUpdateServerListener() {
		updateServerDevice.removeListener(this);
	}
	
	private boolean checkUpdateServer() {
		// Initiate connection with Update Server			
		if (writableConnToUpdateServer == null) {
			writableConnToUpdateServer = us.getWritableConnectionToService(updateServerDID, UpdateDistributionService.SERVICE_NAME, MAX_SECONDS_WAIT_FOR_CONNECTION);
			if (writableConnToUpdateServer == null) {
				us.log("Could not establish connection to UpdateServer. Will not be able to receive updates.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				return false;
			}
		}
		Connection connToUpdateServer = (Connection) writableConnToUpdateServer;
		if(!connToUpdateServer.isOpen()) {
			// The connection is closed so we need to establish a new one.
			writableConnToUpdateServer = us.getWritableConnectionToService(updateServerDID, UpdateDistributionService.SERVICE_NAME, MAX_SECONDS_WAIT_FOR_CONNECTION);
			if (writableConnToUpdateServer == null) {
				us.log("Could not establish connection to UpdateServer. Will not be able to receive updates.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				return false;
			}
		}
		us.log("Writable connection established to UpdateServer. We will now be able to receive updates.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
		return true;
	}
	
	void checkLatestVersion() {
		us.log("Checking latest version.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
		if (checkUpdateServer()) {
			Command cmd = us.getCommand(UpdaterService.COMMAND_OUT_CHECK_LATEST_VERSION); 
			String deviceTypes = UpdaterService.PALCOMSTARTER_DEVICE_TYPE;
			for (String deviceType: monitor.getMonitoredDeviceTypes()) {
				deviceTypes += UpdaterService.PARAM_VALUE_SEPARATOR + deviceType;
			}
			cmd.findParam(UpdaterService.PARAM_DEVICE_TYPE).setData(deviceTypes.getBytes());
			us.sendPalComMessage(writableConnToUpdateServer, cmd);				
		}
	}

	@Override
	public void available(Resource resource) {
		us.log("UpdateServer available again!", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);	
		checkLatestVersion();
	}

	@Override
	public void unavailable(Resource resource) {
		us.log("UpdateServer unavailable :(", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
	}

	@Override
	public void resourceChanged(Resource r) {/* We do not care */}
}