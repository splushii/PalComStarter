package se.lth.cs.palcom.updatedistributionservice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import ist.palcom.resource.descriptor.Command;
import ist.palcom.resource.descriptor.DeviceID;
import ist.palcom.resource.descriptor.PRDService;
import ist.palcom.resource.descriptor.ServiceID;
import se.lth.cs.palcom.communication.connection.Readable;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.device.AbstractDevice;
import se.lth.cs.palcom.logging.Logger;
import se.lth.cs.palcom.service.AbstractSimpleService;
import se.lth.cs.palcom.service.ServiceTools;
import se.lth.cs.palcom.service.command.CommandServiceProtocol;
import se.lth.cs.palcom.service.distribution.UnicastDistribution;
import se.lth.cs.palcom.updaterservice.UpdaterService;

/** 
 * Keeps track of updates and distributes them to devices running {@link UpdaterService} in monitoring mode (PalComStarters)
 * @author Christian Hernvall
 */
public class UpdateDistributionService extends AbstractSimpleService {
	public static final DeviceID CREATOR = new DeviceID("X:mojo");
	public static final ServiceID SERVICE_VERSION = new ServiceID(CREATOR, "UPD1.0.0", CREATOR, "UPD1.0.0");
	public static final String SERVICE_NAME = "UpdateDistributionService";
	
	private static final String COMMAND_IN_BROADCAST_UPDATE_SINGLE_DEVICE_TYPE = "broadcast update for single device type";
	private static final String COMMAND_IN_BROADCAST_UPDATE_MULTIPLE_DEVICES = "broadcast update for multiple device types";
	private static final String COMMAND_IN_LIST_LATEST_UPDATES = "list the latest updates";
	private static final String COMMAND_IN_LIST_ALL_UPDATES = "list all updates";
	private static final String COMMAND_IN_UPDATE_CONTENT_REQUEST = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_UPDATE_CONTENT_REQUEST;
	private static final String COMMAND_IN_CHECK_UPDATE_SERVER = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_CHECK_UPDATE_SERVER;
	private static final String COMMAND_IN_CHECK_LATEST_VERSION = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_CHECK_LATEST_VERSION;
	private static final String COMMAND_IN_BENCHMARK_END = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_OUT_BENCHMARK_END;
	private static final String COMMAND_IN_ADD_UPDATE = "add update";
	private static final String COMMAND_IN_REMOVE_SINGLE_UPDATE = "remove single update";
	private static final String COMMAND_IN_REMOVE_ALL_OLD_UPDATES = "remove all old updates";
	private static final String COMMAND_IN_REMOVE_ALL_UPDATES = "remove all updates";
	
	private static final String COMMAND_OUT_UPDATE_DEVICE_TYPES = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_IN_UPDATE_DEVICE_TYPES;
	private static final String COMMAND_OUT_UPDATE_DATA = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_IN_UPDATE_DATA;
	private static final String COMMAND_OUT_CHECK_UPDATE_SERVER_CONFIRM = se.lth.cs.palcom.updaterservice.UpdaterService.COMMAND_IN_CHECK_UPDATE_SERVER_CONFIRM;
	private static final String COMMAND_OUT_STATUS = "status reply";
	
	private static final String PARAM_VALUE_SEPARATOR = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_VALUE_SEPARATOR;
	private static final String PARAM_IMPLEMENTATION = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_IMPLEMENTATION;
	private static final String PARAM_VERSION = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_VERSION;
	private static final String PARAM_UPDATE_CONTENT = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_UPDATE_CONTENT;
	private static final String PARAM_DEVICE_TYPE = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_DEVICE_TYPE;
	private static final String PARAM_VERSION_ENTRY_UNKNOWN = se.lth.cs.palcom.updaterservice.UpdaterService.PARAM_NO_ENTRY;
	private static final String PARAM_STATUS = "status";
	
	private long benchmark;
	private HashMap<String, String> implementationSuffix;
	
	private UpdateStore updateStore;
	
	public UpdateDistributionService(AbstractDevice container) {
		this(container, ServiceTools.getNextInstance(SERVICE_VERSION));
	}
	
	public UpdateDistributionService(AbstractDevice container, String instance) {
		super(container, SERVICE_VERSION, "P1", "v0.0.1", "UpdateDistributionService",
				instance, "Distributes updates to PalCom devices",
				new UnicastDistribution(true));
		
		implementationSuffix = new HashMap<String, String>();
		implementationSuffix.put("java", ".jar");
		
		try {
			updateStore = new UpdateStore(getServiceRoot());
		} catch (IOException e) {
			Logger.log("Could not access service file system. Exiting.", Logger.CMP_SERVICE, Logger.LEVEL_ERROR);
			stop();
			return;
		}
		if(!updateStore.parseUpdateFileSystem(implementationSuffix.keySet())) {
			stop();
			return;
		}	
		
		CommandServiceProtocol sp = getProtocolHandler();
		
		Command broadcastSingleUpdateCmd = new Command(COMMAND_IN_BROADCAST_UPDATE_SINGLE_DEVICE_TYPE, "Upload specified update and send information about it to all connected clients.", Command.DIRECTION_IN);
		broadcastSingleUpdateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		broadcastSingleUpdateCmd.addParam(PARAM_VERSION, "text/plain");
		broadcastSingleUpdateCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
		sp.addCommand(broadcastSingleUpdateCmd);
		
		Command broadcastMultipleUpdateCmd = new Command(COMMAND_IN_BROADCAST_UPDATE_MULTIPLE_DEVICES, "Send information about the latest updates of all device types to all connected clients.", Command.DIRECTION_IN);
		sp.addCommand(broadcastMultipleUpdateCmd);

		Command addUpdateCmd = new Command(COMMAND_IN_ADD_UPDATE, "Add jar to database", Command.DIRECTION_IN);
		addUpdateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		addUpdateCmd.addParam(PARAM_VERSION, "text/plain");
		addUpdateCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
		sp.addCommand(addUpdateCmd);
		
		Command listLatestUpdatesCmd = new Command(COMMAND_IN_LIST_LATEST_UPDATES, "Lists the latest updates for all device types.", Command.DIRECTION_IN);
		sp.addCommand(listLatestUpdatesCmd);
		
		Command listAllUpdatesCmd = new Command(COMMAND_IN_LIST_ALL_UPDATES, "Lists all updates present.", Command.DIRECTION_IN);
		sp.addCommand(listAllUpdatesCmd);
		
		Command removeUpdateCmd = new Command(COMMAND_IN_REMOVE_SINGLE_UPDATE, "Remove a single update.", Command.DIRECTION_IN);
		removeUpdateCmd.addParam(PARAM_IMPLEMENTATION, "text/plain");
		removeUpdateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		removeUpdateCmd.addParam(PARAM_VERSION, "text/plain");
		sp.addCommand(removeUpdateCmd);
		
		Command removeAllOldUpdatesCmd = new Command(COMMAND_IN_REMOVE_ALL_OLD_UPDATES, "Remove all updates except the latest for each device type.", Command.DIRECTION_IN);
		sp.addCommand(removeAllOldUpdatesCmd);
		
		Command removeAllUpdatesCmd = new Command(COMMAND_IN_REMOVE_ALL_UPDATES, "Remove all updates.", Command.DIRECTION_IN);
		sp.addCommand(removeAllUpdatesCmd);
		
		Command updateContentRequestCmd = new Command(COMMAND_IN_UPDATE_CONTENT_REQUEST, "Update content request.", Command.DIRECTION_IN);
		updateContentRequestCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		updateContentRequestCmd.addParam(PARAM_VERSION, "text/plain");
		sp.addCommand(updateContentRequestCmd);
	
		Command checkUpdateServerCmd = new Command(COMMAND_IN_CHECK_UPDATE_SERVER, "Confirmation request from client.", Command.DIRECTION_IN);
		sp.addCommand(checkUpdateServerCmd);
		
		Command latestVersionRequestCmd = new Command(COMMAND_IN_CHECK_LATEST_VERSION, "Latest version request from client.", Command.DIRECTION_IN);
		latestVersionRequestCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		sp.addCommand(latestVersionRequestCmd);
		
		Command statusCmd = new Command(COMMAND_OUT_STATUS, "Generic status reply.", Command.DIRECTION_OUT);
		statusCmd.addParam(PARAM_STATUS, "text/plain");
		sp.addCommand(statusCmd);

		Command updateCmd = new Command(COMMAND_OUT_UPDATE_DEVICE_TYPES, "", Command.DIRECTION_OUT);
		updateCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		updateCmd.addParam(PARAM_VERSION, "text/plain");
		sp.addCommand(updateCmd);
		
		Command updateContentCmd = new Command(COMMAND_OUT_UPDATE_DATA, "Reply with content to content request.", Command.DIRECTION_OUT);
		updateContentCmd.addParam(PARAM_DEVICE_TYPE, "text/plain");
		updateContentCmd.addParam(PARAM_VERSION, "text/plain");
		updateContentCmd.addParam(PARAM_UPDATE_CONTENT, "application/x-jar");
		sp.addCommand(updateContentCmd);

		Command confirmReqCmd = new Command(COMMAND_OUT_CHECK_UPDATE_SERVER_CONFIRM, "Confirmation reply to confirmation request.", Command.DIRECTION_OUT);
		sp.addCommand(confirmReqCmd);
		
		
		Command benchmarkEndCmd = new Command(COMMAND_IN_BENCHMARK_END, "benchmark end", Command.DIRECTION_IN);
		sp.addCommand(benchmarkEndCmd);
	}

	@Override
	protected void invoked(Readable connection, Command command) {
		if (connection instanceof Writable) {
			Writable conn = (Writable) connection;
			if(command.getID().equals(COMMAND_IN_BROADCAST_UPDATE_SINGLE_DEVICE_TYPE)) {
				benchmark = System.currentTimeMillis();
				Logger.log("Benchmarking time to update. Current time: " + benchmark, Logger.CMP_SERVICE, Logger.LEVEL_BULK);
				String implementation = "java"; //TODO hard coded implementation
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData());
				byte[] content = command.findParam(PARAM_UPDATE_CONTENT).getData();
				Logger.log("Saving update " + deviceType + "(v" + version + ")", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				if (saveExecutable(implementation, deviceType, version, content.clone())) {
					Logger.log("Broadcasting update info to devices.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
					announceNewUpdate(new String[] {implementation}, new String[] {deviceType}, new String[] {version});					
				} else {
					return;
				}
			} else if (command.getID().equals(COMMAND_IN_UPDATE_CONTENT_REQUEST)) {
				String implementation = "java"; // TODO hard coded implementation
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData());
				if(replyWithJar(implementation, deviceType, version, conn)) {
					Logger.log("Replying with update content (v" + version + ") to " + deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_INFO);					
				} else {
					Logger.log("Could not reply with update content (v" + version + ") to " + deviceType + ".", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
				}
			} else if (command.getID().equals(COMMAND_IN_CHECK_UPDATE_SERVER)) {				
				Logger.log("Replying to a device checking its connection to me.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
				replyToConfirmRequest(conn);
			} else if (command.getID().equals(COMMAND_IN_CHECK_LATEST_VERSION)) {
				Logger.log("Replying with latest version info to device.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				String deviceTypes = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				replyWithLatestVersion(conn, deviceTypes);
			} else if (command.getID().equals(COMMAND_IN_BENCHMARK_END)) {
				Logger.log("Got benchmark end command. Current time: " + System.currentTimeMillis(), Logger.CMP_SERVICE, Logger.LEVEL_BULK);
				Logger.log("Difference between start and now: " + (System.currentTimeMillis() - benchmark), Logger.CMP_SERVICE, Logger.LEVEL_BULK);
			} else if (command.getID().equals(COMMAND_IN_ADD_UPDATE)) {
				String implementation = "java"; //TODO hard coded implementation
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData());
				byte[] content = command.findParam(PARAM_UPDATE_CONTENT).getData();
				Logger.log("Adding " + deviceType + " version " + version + " to update database.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				if (!saveExecutable(implementation, deviceType, version, content))
					return;
			} else if (command.getID().equals(COMMAND_IN_BROADCAST_UPDATE_MULTIPLE_DEVICES)) {
				benchmark = System.currentTimeMillis();
				Logger.log("Benchmarking time to update. Current time: " + benchmark, Logger.CMP_SERVICE, Logger.LEVEL_BULK);
				
				Set<UpdateEntry> latestUpdates = updateStore.getLatestUpdates();
				int size = latestUpdates.size();
				String[] implementationTypes = new String[size];
				String[] deviceTypes = new String[size];
				String[] versions = new String[size];
				String msg = "Broadcasting update for device types:";
				int i = 0;
				for (UpdateEntry updateEntry: latestUpdates) {
						implementationTypes[i] = updateEntry.implementation;
						deviceTypes[i] = updateEntry.deviceType;
						versions[i] = updateEntry.version;
						msg += " " + deviceTypes[i];
						i++;
				}
				Logger.log(msg, Logger.CMP_SERVICE, Logger.LEVEL_INFO);
				announceNewUpdate(implementationTypes, deviceTypes, versions);
			} else if (command.getID().equals(COMMAND_IN_LIST_LATEST_UPDATES)) {
				String latestUpdates = updateStore.getLatestUpdatesInText();
				Command reply = getProtocolHandler().findCommand(COMMAND_OUT_STATUS);
				reply.findParam(PARAM_STATUS).setData(latestUpdates.getBytes());
				sendTo(conn, reply);
			} else if (command.getID().equals(COMMAND_IN_LIST_ALL_UPDATES)) {
				String allUpdates = updateStore.getAllUpdatesInText();
				Command reply = getProtocolHandler().findCommand(COMMAND_OUT_STATUS);
				reply.findParam(PARAM_STATUS).setData(allUpdates.getBytes());
				sendTo(conn, reply);
			} else if (command.getID().equals(COMMAND_IN_REMOVE_SINGLE_UPDATE)) {
				String implementation = UpdaterService.toUTF8String(command.findParam(PARAM_IMPLEMENTATION).getData());
				String deviceType = UpdaterService.toUTF8String(command.findParam(PARAM_DEVICE_TYPE).getData());
				String version = UpdaterService.toUTF8String(command.findParam(PARAM_VERSION).getData());
				updateStore.deleteUpdate(implementation, deviceType, version);
			} else if (command.getID().equals(COMMAND_IN_REMOVE_ALL_OLD_UPDATES)) {
				updateStore.deleteAllOldUpdates();
			} else if (command.getID().equals(COMMAND_IN_REMOVE_ALL_UPDATES)) {
				updateStore.deleteAllUpdates();
			}
		}
	}
	
	private boolean saveExecutable(String implementation, String deviceType, String version, byte[] content) {
		return updateStore.saveUpdate(implementation, deviceType, version, implementationSuffix.get(implementation), content);
	}

	private void replyWithLatestVersion(Writable conn, String deviceTypes) {
		String[] splitDeviceTypes = deviceTypes.split(PARAM_VALUE_SEPARATOR);
		String versions = null;
		for (String deviceType: splitDeviceTypes) {
			UpdateEntry updateEntry = updateStore.getLatestUpdate("java", deviceType);
			String latestVersion = null;
			if (updateEntry == null) {
				latestVersion = PARAM_VERSION_ENTRY_UNKNOWN;
			} else {
				latestVersion = updateEntry.version;
			}
			
			if (versions != null) {
				versions += PARAM_VALUE_SEPARATOR;
			} else {
				versions = "";
			}
			versions += latestVersion;
		}
		if (versions != null) {
			Command reply = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_DEVICE_TYPES);
			reply.findParam(PARAM_DEVICE_TYPE).setData(deviceTypes.getBytes());
			reply.findParam(PARAM_VERSION).setData(versions.getBytes());
			sendTo(conn, reply);
		}
	}

	private void replyToConfirmRequest(Writable conn) {
		Command reply = getProtocolHandler().findCommand(COMMAND_OUT_CHECK_UPDATE_SERVER_CONFIRM);
		sendTo(conn, reply);
	}

	private boolean replyWithJar(String implementation, String deviceType, String version, Writable conn) {
		UpdateEntry updateEntry = updateStore.getUpdate(implementation, deviceType, version);	
		if (updateEntry == null) {
			Logger.log("Could not find update: " + implementation + " " + deviceType + " " + version, Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		byte[] content;
		try {
			content = updateEntry.executableFile.getContents();
		} catch (IOException e1) {
			Logger.log("Could not access jar with version " + version + " for the client.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		if (content == null) {
			Logger.log("Could not find jar with version " + version + " for the client.", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		
		Logger.log("Sending " + deviceType + " " + version + " content.", Logger.CMP_SERVICE, Logger.LEVEL_DEBUG);
		Command reply = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_DATA);
		reply.findParam(PARAM_DEVICE_TYPE).setData(deviceType.getBytes());
		reply.findParam(PARAM_VERSION).setData(version.getBytes());
		reply.findParam(PARAM_UPDATE_CONTENT).setData(content);
		try {
			 blockingSendTo(conn, reply);
		} catch (InterruptedException e) {
			Logger.log("Could not send update data to client: SEND_ERROR", Logger.CMP_SERVICE, Logger.LEVEL_WARNING);
			return false;
		}
		return true;
	}

	private void announceNewUpdate(String[] implementationTypes, String[] deviceTypes, String[] versions) { //TODO send implementation types
		if (deviceTypes.length == 0) {
			Logger.log("No updates to announce. Use \"" + COMMAND_IN_ADD_UPDATE + "\" command to add an update.", Logger.CMP_SERVICE, Logger.LEVEL_INFO);
			return;
		}
		Command cmd = getProtocolHandler().findCommand(COMMAND_OUT_UPDATE_DEVICE_TYPES);
		String concDeviceTypes = null;
		String concVersions = null;
		for (int i = 0; i < deviceTypes.length; ++i) {
			if (concVersions != null) {
				concDeviceTypes += PARAM_VALUE_SEPARATOR;
				concVersions += PARAM_VALUE_SEPARATOR;
			} else {
				concDeviceTypes = "";
				concVersions = "";
			}
			concDeviceTypes += deviceTypes[i];
			concVersions += versions[i];
		}
		cmd.findParam(PARAM_DEVICE_TYPE).setData(concDeviceTypes.getBytes());
		cmd.findParam(PARAM_VERSION).setData(concVersions.getBytes());
		sendToAll(cmd);
	}

	public void start() {
		setStatus(PRDService.FULLY_OPERATIONAL);
		super.start();
	}
}
