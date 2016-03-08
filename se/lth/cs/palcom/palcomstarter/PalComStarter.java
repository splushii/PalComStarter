package se.lth.cs.palcom.palcomstarter;

import java.io.File;
import java.io.IOException;

import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.device.AbstractDevice;
import se.lth.cs.palcom.filesystem.HostFileSystems;
import se.lth.cs.palcom.service.FSServiceManager;
import se.lth.cs.palcom.updaterservice.UpdaterService;
import se.lth.cs.palcom.util.configuration.DeviceList;
import se.lth.cs.palcom.util.configuration.DeviceList.DeviceItem;
import se.lth.cs.palcom.logging.Logger;

public class PalComStarter extends AbstractDevice {
	private static final String VERSION = "1.0.0";
	private static final String COM_HELP = "h";
	private final static String COM_DEVICE = "x";
	private static final String COM_FILE_SYSTEM = "f";
	
	public final static String DEVICE_TYPE = "PalComStarter";
	public static final String COM_CONTINUE_UPDATE_STAGE_THREE = "-continue-update-stage-three";

	public PalComStarter(DeviceItem deviceItem, String version, boolean continueUpdateStageThree) {
		super(deviceItem.getID(), deviceItem.getName(), version);
		Logger.setLevel(Logger.LEVEL_WARNING);
		FSServiceManager sm = new FSServiceManager(this, Thread.currentThread().getContextClassLoader());
		sm.enableManagerService();
//		try {
//			sm.start(true);
//		} catch (IllegalArgumentException e) {
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//		} catch (InvocationTargetException e) {
//			e.printStackTrace();
//		}
		UpdaterService us = new UpdaterService(this, continueUpdateStageThree);
		us.register();
		us.start();
	}
	
	public static void main(String[] args) {
		DeviceID deviceID = null;
		String palcomFSPath = null;
		boolean continueUpdateStageThree = false;
		
		System.out.println("Arguments:");
		for (String arg: args) {
			System.out.println(arg);
		}
		System.out.println();
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("-x")) {
				if (args.length > i + 1) {
					deviceID = new DeviceID(args[i + 1]);					
				}
			} else if (args[i].equals("-"+COM_FILE_SYSTEM)) {
				// Check path etc ...
				if (args.length > i + 1) {
					palcomFSPath = args[i + 1];
					try{
						File f = new File(palcomFSPath);
						if(f.isAbsolute()&&f.isDirectory()){
							HostFileSystems.setDeviceRootPath(palcomFSPath);
						} else {
							System.err.println("Incorrect usage, -" + COM_FILE_SYSTEM + " " +palcomFSPath
									+ " must be a valid path to an existing directory");
							printUsage();
							System.out.println("Exiting");
							System.exit(1);
						}
					}catch(Exception ex){
						System.err.println("Incorrect usage, -" + COM_FILE_SYSTEM + " " +palcomFSPath
								+ " must be a valid path to an existing directory");
						printUsage();
						System.out.println("Exiting");
						System.exit(1);
					}
					System.out.println(" -f using path: "+palcomFSPath);												
				} else{
					System.err.println("-" + COM_FILE_SYSTEM + " used without specifying a path.");
					printUsage();
					System.err.println("Exiting");
					System.exit(1);
				}
			} else if (args[i].equals("-" + COM_CONTINUE_UPDATE_STAGE_THREE)) {
				continueUpdateStageThree = true;
				System.out.println("Will continue in stage three.");
			} else if (args[i].equals("-" + COM_HELP)) {
				printUsage();
				System.exit(0);
			}
		}
		
		DeviceItem[] configurations = null;
		DeviceItem deviceItem = null;
		if (deviceID == null) {
			System.out.println("Device ID not set. Choosing the first default device configuration. Generates a new configuration if none is present.");
			try {
				configurations = DeviceList.getConfigurations(DEVICE_TYPE, true);
			} catch (IOException ex) {
				System.err.println("Failed to access default device ID. Cannot start device.");
				System.err.println(ex.getMessage());
				System.exit(1);
			}
			deviceItem = configurations[0];
		} else {
			try {
				configurations = DeviceList.getConfigurations(DEVICE_TYPE, false);
			} catch (IOException e) {
				System.err.println("Failed to access default device ID. Cannot start device.");
				System.exit(1);
			}
			deviceItem = getDeviceItem(configurations, deviceID.getID());
			if (deviceItem == null) {
				System.err.println("Unable to find the configuration with id: "
						+ deviceID.getID());
				System.out.println("Exiting");
				System.exit(1);
			}
		}
		
		if (palcomFSPath == null) {
			try {
				System.out.println("PalCom Filesystem path not set. Will use default path: " + HostFileSystems.getGlobalRoot().getURL().replace("global", ""));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		new PalComStarter(deviceItem, VERSION, continueUpdateStageThree).start();
	}
	
	private static void printUsage() {
		System.out.println(" --- PalComStarter options ---");
		System.out
				.println("  -"+COM_HELP+"                                      --- Print this help");
		System.out
				.println("  -"+COM_DEVICE+" <device id>                          --- Use PalComStarter with device id <device id>.");
		System.out
				.println("  -"+COM_FILE_SYSTEM+" <dir-path>                           --- Start PalComStarter with PalcomFilesystem located in existing directory at <dir-path>");
		System.out
				.println("  -"+COM_CONTINUE_UPDATE_STAGE_THREE+"                           --- Start PalComStarter in update stage three in order to continue an ongoing updating process. This flag should only be set manually during testing. DO NOT USE UNLESS YOU KNOW WHAT IT DOES!");

	}
	
	private static DeviceItem getDeviceItem(DeviceItem[] configurations,
			String deviceId) {
		for (int i = 0; i < configurations.length; i++) {
			if (configurations[i].getID().getID().equals(deviceId)) {
				return configurations[i];
			}
		}
		return null;
	}
}
