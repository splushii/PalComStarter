package se.lth.cs.palcom.updaterservice;

import ist.palcom.resource.descriptor.DeviceID;
import se.lth.cs.palcom.communication.connection.Writable;
import se.lth.cs.palcom.discovery.proxy.PalcomDevice;

/**
 *	Datastructure used by MonitoringThread and during update, to keep track 
 *	of monitored devices and its related attributes.
 *	@author Christian Hernvall
 */
class MonitoredDevice {
	DeviceID deviceID;
	String deviceType;
	PalcomDevice palcomDevice;
	Writable conn;
	Process p;
	int nextProtocolBreakingUpdateVersion = -1;
	long recentlyStartedDelay = 0;
	MonitoredDevice(DeviceID deviceID, String typeOfDevice, PalcomDevice palcomDevice) {
		this.deviceID = deviceID;
		this.deviceType = typeOfDevice;
		this.palcomDevice = palcomDevice;
	}
}