package se.lth.cs.palcom.updaterservice;

import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.discovery.proxy.PalcomDevice;

// +----------------------------------------------------------------------------------------------+
// |                          MonitoredDevice                                                     |
// +----------------------------------------------------------------------------------------------+
/**
 *	Datastructure used by MonitoringThread and during update, to keep track 
 *	of monitored devices and its related attributes.
 *	@author Christian Hernvall
 */
public class MonitoredDevice {
	DeviceID deviceID;
//	String pathToJar;
	String deviceType;
	PalcomDevice palcomDevice;
	Writable conn;
	Process p;
	long recentlyStartedDelay = 0;
	public MonitoredDevice(DeviceID deviceID, String typeOfDevice, PalcomDevice palcomDevice) {
		this.deviceID = deviceID;
//		this.pathToJar = pathToJar;
		this.deviceType = typeOfDevice;
		this.palcomDevice = palcomDevice;
	}
}