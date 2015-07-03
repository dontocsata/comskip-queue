package com.dontocsata.comskip.queue;

public class WtvMetaData {

	private String name;
	private MetaDataType type;
	private Object value;

	public WtvMetaData(String name, MetaDataType type, Object value) {
		super();
		this.name = name;
		this.type = type;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public MetaDataType getType() {
		return type;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "WtvMetaData [name=" + name + ", type=" + type + ", value=" + value + "]";
	}

	public static enum MetaDataType {
		INTEGER(0x00),
		STRING(0x01),
		BOOLEAN(0x03),
		LONG(0x04),
		GUID(0x06);

		private int code;

		private MetaDataType(int code) {
			this.code = code;
		}

		public static MetaDataType fromInt(int code) {
			for (MetaDataType type : values()) {
				if (type.code == code) {
					return type;
				}
			}
			throw new IllegalArgumentException("Unsupport meta data type with code: " + code);
		}
	}
}
