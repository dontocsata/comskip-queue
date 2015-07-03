package com.dontocsata.comskip.queue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.dontocsata.comskip.queue.WtvMetaData.MetaDataType;

public class WtvMetaParser {

	private static byte[] HEADER_BYTES;

	static {
		short[] headerCode = new short[] { 0x5A, 0xFE, 0xD7, 0x6D, 0xC8, 0x1D, 0x8F, 0x4A, 0x99, 0x22, 0xFA, 0xB1, 0x1C,
				0x38, 0x14, 0x53 };
		HEADER_BYTES = new byte[headerCode.length];
		ByteBuffer buff = ByteBuffer.wrap(HEADER_BYTES);
		for (short s : headerCode) {
			buff.put((byte) (s & 0xff));
		}
	}

	public static Map<String, WtvMetaData> parse(File file) throws IOException {
		Map<String, WtvMetaData> toRet = new HashMap<>();
		try (RandomAccessFile raf = new RandomAccessFile(file, "r"); FileChannel channel = raf.getChannel()) {
			channel.position(0x12000);
			while (checkHeader(channel)) {
				ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
				channel.read(buffer);
				buffer.flip();
				MetaDataType type = MetaDataType.fromInt(buffer.getInt());
				int length = buffer.getInt();
				StringBuilder sb = new StringBuilder();
				byte[] b = new byte[2];
				ByteBuffer temp = ByteBuffer.wrap(b);
				while (true) {
					channel.read(temp);
					temp.flip();
					String s = new String(b, Charset.forName("UTF-16LE"));
					if (s.charAt(0) != '\0') {
						sb.append(s);
					} else {
						break;
					}
				}
				String title = sb.toString();
				ByteBuffer dataBuff = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
				channel.read(dataBuff);
				dataBuff.flip();
				switch (type) {
					case INTEGER:
						toRet.put(title, new WtvMetaData(title, type, dataBuff.getInt()));
						break;
					case STRING:
						toRet.put(title, new WtvMetaData(title, type,
								new String(dataBuff.array(), Charset.forName("UTF-16LE"))));
						break;
					case BOOLEAN:
						toRet.put(title, new WtvMetaData(title, type, dataBuff.get() != 0));
						break;
					case LONG:
						toRet.put(title, new WtvMetaData(title, type, dataBuff.getLong()));
						break;
					case GUID:
						String guid = bytesToHex(dataBuff.array());
						toRet.put(title, new WtvMetaData(title, type, guid));
						break;
				}
			}
		}
		return toRet;
	}

	final protected static char[] hexArray = "0123456789abcdef".toCharArray();

	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	private static boolean checkHeader(FileChannel channel) throws IOException {
		byte[] bytes = new byte[HEADER_BYTES.length];
		ByteBuffer headerBytes = ByteBuffer.wrap(bytes);
		channel.read(headerBytes);
		return Arrays.equals(bytes, HEADER_BYTES);
	}
}
