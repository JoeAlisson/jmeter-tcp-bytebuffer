package com.github.joealisson.sampler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Client {

    private SocketChannel socket;

    public void connect(String server, int port) throws IOException {
        socket = SocketChannel.open(new InetSocketAddress(server, port));
    }

    public void send(ByteBuffer buffer) throws IOException {
        socket.write(buffer);
    }


    public void read(ByteBuffer byteBuffer) throws IOException {
        socket.read(byteBuffer);
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
