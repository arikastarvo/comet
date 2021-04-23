package com.github.arikastarvo.comet.utils;

import java.net.UnknownHostException;

import org.xbill.DNS.Address;

public class DNSUtil {

	/**
	 * 
	 * TODO! IMPLEMENT CACHE!! dnsjava has a dedicated class for it, but it must be used manually (add, check, clear etc)
	 * 
	 * @param ip
	 * @return
	 */
	public static String dns_lookup(String ip) {
		
		if(Address.isDottedQuad(ip)) {
			try {
				return Address.getHostName(Address.getByAddress(ip));
			} catch (UnknownHostException e) {
				return ip;
			}
		} else {
			try {
				return Address.getByName(ip).getHostAddress();
			} catch (UnknownHostException e) {
				return ip;
			}
		}
	}
}
