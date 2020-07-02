package io.vertx.ext.web.handler.sse.impl;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;

class SSEPacket {

	/* Use constants, but hope this will never change in the future (it should'nt) */
	public static final String LINE_SEPARATOR = "\n";
	private static final String FIELD_SEPARATOR = ":";

	private final StringBuilder payload;

	String eventType;
	private String field;
	private String value;

	String lastEvenId;
	int reconnectionTime;
	private boolean dispatchEvent;
	private RecordParser recordParser;

	SSEPacket() {
		recordParser = RecordParser.newDelimited("\n");
		recordParser.handler( h -> parseLine(h.toString()));
		payload = new StringBuilder(1024);
	}

	void append(Buffer buffer) {
		recordParser.handle(buffer);
	}

	private void parseLine(String line) {
		if (line.isEmpty())
			dispatchEvent();
		if (line.startsWith(FIELD_SEPARATOR)) {
			return;
		}
		if (line.contains(FIELD_SEPARATOR)) {
			String split[] = line.split(FIELD_SEPARATOR);
			field = split[0].trim();
			value = split[1].trim();
			processField(field, value);
		}
		else {
			field = line;
			value = "";
		}
	}

	private void processField(String field, String value) {
		switch (field) {
			case "event":
				eventType = value;
				break;
			case "data":
				payload.append(value).append(LINE_SEPARATOR);
				break;
			case "id":
				lastEvenId = value;
				break;
			case "retry":
				reconnectionTime = Integer.valueOf(value);
				break;
			default:
				break;
		}
	}

	private void dispatchEvent() {
		dispatchEvent = true;
	}

	@Override
	public String toString() {
		return payload == null ? "" : payload.toString();
	}

	public boolean isComplete() {
		return dispatchEvent;
	}

	public boolean isDataEmpty() {
		return payload.toString().isEmpty();
	}
}
