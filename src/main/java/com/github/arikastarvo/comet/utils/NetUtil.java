package com.github.arikastarvo.comet.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

import inet.ipaddr.IPAddressSeqRange;
import inet.ipaddr.IPAddressString;

public class NetUtil {

	
	static IPAddressString net_1 = new IPAddressString("172.16.0.0/12");	// private local
	static IPAddressString net_2 = new IPAddressString("192.168.0.0/16");	// private local
	static IPAddressString net_3 = new IPAddressString("10.0.0.0/8");		// private local
	static IPAddressString net_4 = new IPAddressString("169.254.0.0/16");	// link-local
	static IPAddressString net_5 = new IPAddressString("127.0.0.0/8");		// loopback
	
	public static boolean ipin(String ip, String network) {
		if(ip == null || ip.length() == 0) {
			return false;
		}
		IPAddressString netObj = new IPAddressString(network);
		IPAddressString ipObj = new IPAddressString(ip);
		return netObj.contains(ipObj);
	}
	
	public static boolean isprivateip(String ip) {
		if(ip == null || ip.length() == 0) {
			return false;
		}
		IPAddressString ipObj = new IPAddressString(ip);
		
		return net_1.contains(ipObj) || net_2.contains(ipObj) || net_3.contains(ipObj) || net_4.contains(ipObj) || net_5.contains(ipObj);
	}

	public static boolean notprivateip(String ip) {
		if(ip == null || ip.length() == 0) {
			return false;
		}
		IPAddressString ipObj = new IPAddressString(ip);

		return !net_1.contains(ipObj) && !net_2.contains(ipObj) && !net_3.contains(ipObj) && !net_4.contains(ipObj) && !net_5.contains(ipObj);
	}
	
	public static IPAddressSeqRange netrange(String network) {
		return new IPAddressString(network).getSequentialRange();
	}
	
	public static long ipToLong(String ip) {
		try {
			return ipToLong(InetAddress.getByName(ip));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
		}
		return 0;
	}
	
	public static long ipToLong(InetAddress ip) {
		byte[] octets = ip.getAddress();
		long result = 0;
		for (byte octet : octets) {
			result <<= 8;
			result |= octet & 0xff;
		}
		return result;
	}
}
