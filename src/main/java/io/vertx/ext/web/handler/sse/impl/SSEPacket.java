package io.vertx.ext.web.handler.sse.impl;

import io.vertx.core.buffer.Buffer;

import java.util.stream.Stream;

class SSEPacket {

	/* Use constants, but hope this will never change in the future (it should'nt) */
	private static final String END_OF_PACKET = "\n\n";
	public static final String LINE_SEPARATOR = "\n";
	private static final String FIELD_SEPARATOR = ":";

	private final StringBuilder payload;
	private StringBuilder bufferStream;
	private int beginBuffer = 0;
	private int endBuffer = 0;

	String eventType;
	private String field;
	private String value;

	String lastEvenId;
	int reconnectionTime;
	private boolean dispatchEvent;

	SSEPacket() {
		payload = new StringBuilder();
		//TODO : Implement circular buffer
		bufferStream = new StringBuilder();
	}

	void append(Buffer buffer) {
		String response = buffer.toString();
		bufferStream.append(response);
		endBuffer = bufferStream.length() -1;

		int idxEndOfLine = bufferStream.indexOf("\n", beginBuffer);
		while (idxEndOfLine != -1) {
			parseLine(beginBuffer, idxEndOfLine);
			beginBuffer = idxEndOfLine + 1;
			idxEndOfLine = bufferStream.indexOf("\n", beginBuffer);
		}
	}

	private void parseLine(int begin, int end) {
		String line = bufferStream.substring(begin,end);
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
