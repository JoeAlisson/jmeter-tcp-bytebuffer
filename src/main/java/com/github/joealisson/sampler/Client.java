package com.github.joealisson.sampler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class Client {

    private SocketChannel socket;

    void connect(String server, int port) throws IOException {
        socket = SocketChannel.open(new InetSocketAddress(server, port));
    }

    void send(ByteBuffer buffer) throws IOException {
        socket.write(buffer);
    }


    void read(ByteBuffer byteBuffer) throws IOException {
        socket.read(byteBuffer);
    }

    void close() {
        try {
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
