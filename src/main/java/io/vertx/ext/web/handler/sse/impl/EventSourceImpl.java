package io.vertx.ext.web.handler.sse.impl;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.handler.sse.EventSource;
import io.vertx.ext.web.handler.sse.exceptions.ConnectionRefusedException;

import java.util.HashMap;
import java.util.Map;

public class EventSourceImpl implements EventSource {

	private HttpClient client;
	private boolean connected;
	private String lastId;
	private Handler<String> messageHandler;
	private final Map<String, Handler<String>> eventHandlers;
	private SSEPacket currentPacket;
	private final Vertx vertx;
	private final HttpClientOptions options;
	private Handler<Void> closeHandler;
	private RecordParser recordParser;

	public EventSourceImpl(Vertx vertx, HttpClientOptions options) {
		options.setKeepAlive(true);
		this.vertx = vertx;
		this.options = options;
		eventHandlers = new HashMap<>();
		this.recordParser = RecordParser.newDelimited("\n");
	}

	@Override
	public EventSource connect(String path, Handler<AsyncResult<Void>> handler) {
		return connect(path, null, handler);
	}

	@Override
	public EventSource connect(String path, String lastEventId, Handler<AsyncResult<Void>> handler) {
		if (connected) {
			throw new VertxException("SSEConnection already connected");
		}
		if (client == null) {
			client = vertx.createHttpClient(options);
		}
		HttpClientRequest request = client.get(path, response -> {
			if (response.statusCode() != 200) {
				ConnectionRefusedException ex = new ConnectionRefusedException(response);
				handler.handle(Future.failedFuture(ex));
			} else {
				connected = true;
				response.handler(this::handleMessage);
				if (closeHandler != null) {
					response.endHandler(closeHandler);
				}
				handler.handle(Future.succeededFuture());
			}
		});
		if (lastEventId != null) {
			request.headers().add("Last-Event-ID", lastEventId);
		}
		request.setChunked(true);
		request.headers().add(HttpHeaders.ACCEPT, "text/event-stream");
		request.end();
		return this;
	}

	@Override
	public EventSource close() {
		client.close();
		client = null;
		connected = false;
		return this;
	}

	@Override
	public EventSource onMessage(Handler<String> messageHandler) {
		this.messageHandler = messageHandler;
		return this;
	}

	@Override
	public EventSource onEvent(String eventName, Handler<String> handler) {
		eventHandlers.put(eventName, handler);
		return this;
	}

	@Override
	public EventSource onClose(Handler<Void> closeHandler) {
		this.closeHandler = closeHandler;
		return this;
	}

	@Override
	public String lastId() {
		return lastId;
	}

	private void handleMessage(Buffer buffer) {
		if (currentPacket == null) {
			currentPacket = new SSEPacket();
			recordParser.handler(currentPacket);
		}

		recordParser.handle(buffer);
		if (currentPacket.isComplete()) {
			lastId = currentPacket.lastEvenId;

			if (currentPacket.isDataEmpty()) {
				return;
			}

			Handler<String> handler;
			if (!currentPacket.eventType.isEmpty()) {
				handler = eventHandlers.get(currentPacket.eventType);
				handler.handle(currentPacket.toString());
			}
			else {
				messageHandler.handle(currentPacket.toString());
			}
			currentPacket = null;
		}
	}
}
