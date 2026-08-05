package com.example.baseline.utils.python;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;

final class PythonRuntimeProtocol {
    static final int VERSION = 1;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };

    private PythonRuntimeProtocol() {
    }

    static void writeFrame(SocketChannel channel, Map<String, Object> metadata, byte[] body, int maxFrameBytes)
            throws IOException {
        byte[] metadataBytes = OBJECT_MAPPER.writeValueAsBytes(metadata);
        byte[] content = body == null ? new byte[0] : body;
        int payloadLength = Integer.BYTES + metadataBytes.length + content.length;
        if (payloadLength <= Integer.BYTES || payloadLength > maxFrameBytes) {
            throw new IOException("Protocol frame size is outside the configured limit: " + payloadLength);
        }
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + payloadLength);
        frame.putInt(payloadLength);
        frame.putInt(metadataBytes.length);
        frame.put(metadataBytes);
        frame.put(content);
        frame.flip();
        writeFully(channel, frame);
    }

    static Frame readFrame(SocketChannel channel, int maxFrameBytes) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        readFully(channel, lengthBuffer);
        lengthBuffer.flip();
        int payloadLength = lengthBuffer.getInt();
        if (payloadLength <= Integer.BYTES || payloadLength > maxFrameBytes) {
            throw new IOException("Invalid protocol frame length: " + payloadLength);
        }
        ByteBuffer payload = ByteBuffer.allocate(payloadLength);
        readFully(channel, payload);
        payload.flip();
        int metadataLength = payload.getInt();
        if (metadataLength <= 0 || metadataLength > payloadLength - Integer.BYTES) {
            throw new IOException("Invalid protocol metadata length: " + metadataLength);
        }
        byte[] metadataBytes = new byte[metadataLength];
        payload.get(metadataBytes);
        int bodyLength = payloadLength - Integer.BYTES - metadataLength;
        byte[] body = new byte[bodyLength];
        payload.get(body);
        Map<String, Object> metadata = OBJECT_MAPPER.readValue(
                new String(metadataBytes, StandardCharsets.UTF_8), MAP_TYPE);
        Object version = metadata.get("protocolVersion");
        if (!(version instanceof Number number) || number.intValue() != VERSION) {
            throw new IOException("Unsupported protocol version: " + version);
        }
        if (!(metadata.get("type") instanceof String)) throw new IOException("Protocol message type is required");
        return new Frame(metadata, body);
    }

    private static void readFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) throw new EOFException("Python runtime connection closed");
        }
    }

    private static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) channel.write(buffer);
    }

    record Frame(Map<String, Object> metadata, byte[] body) {
        String type() {
            return (String) metadata.get("type");
        }
    }
}
