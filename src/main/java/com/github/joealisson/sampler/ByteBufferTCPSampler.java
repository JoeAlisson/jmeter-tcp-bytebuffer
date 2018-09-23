package com.github.joealisson.sampler;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.ThreadListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

import static java.util.Objects.isNull;


public class ByteBufferTCPSampler extends AbstractJavaSamplerClient implements ThreadListener, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferTCPSampler.class);
    private static final String SERVER = "SERVER";
    private static final String PORT = "PORT";
    private static final String BUFFER_SIZE = "BUFFER_SIZE";
    private static final String SEND_DIRECT = "SEND_DIRECT";
    private static final String BYTE_ORDER = "BYTE_ORDER";

    private Client client;
    private ByteBuffer sampleBuffer;
    private boolean defaultPacket = false;
    private ByteBuffer rBuffer;

    public ByteBufferTCPSampler() {
        client =  new Client();
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(SERVER,"127.0.0.1");
        defaultParameters.addArgument(PORT,"8585");
        defaultParameters.addArgument(BUFFER_SIZE, "20");
        defaultParameters.addArgument(SEND_DIRECT, "true");
        defaultParameters.addArgument(BYTE_ORDER, "little endian");
        return defaultParameters;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        String server = javaSamplerContext.getParameter(SERVER);
        int port = javaSamplerContext.getIntParameter(PORT);

        SampleResult result = new SampleResult();

        rBuffer = getReadBuffer(javaSamplerContext);

        if(isNull(sampleBuffer)) {

            createSampleBuffer(javaSamplerContext);
            putDefaultPacketOnBuffer(javaSamplerContext);
        }

        result.sampleStart();
        try {
            client.connect(server, port);
            client.send(getSampleBuffer());
            client.read(rBuffer);

            if(defaultPacket) {
                makeDefaultResponse(result);
            } else {
                result.sampleEnd();
                result.setSuccessful(true);
                result.setResponseCodeOK();
            }

        } catch (IOException e) {
            LOGGER.error("Request was not successfully processed",e);
            result.sampleEnd();
            result.setResponseMessage(e.getMessage());
            result.setSuccessful(false);
        } finally {
            if(client != null) {
                client.close();
            }
            client = new Client();
        }
        return result;
    }

    private void makeDefaultResponse(SampleResult result) {
        rBuffer.flip();
        while (rBuffer.remaining() >= 8) {
            rBuffer.getLong();
        }

        while (rBuffer.remaining() >= 4) {
            rBuffer.getInt();
        }

        while (rBuffer.remaining() >= 2) {
            rBuffer.getShort();
        }

        while (rBuffer.remaining() >= 1) {
            rBuffer.get();
        }
        result.sampleEnd();
        result.setSuccessful(true);
        result.setResponseCodeOK();
        result.setResponseMessage((result.getEndTime() - result.getStartTime()) + " ms ");
    }

    private ByteBuffer getReadBuffer(JavaSamplerContext javaSamplerContext) {
        if(isSendDirect(javaSamplerContext)) {
            return ByteBuffer.allocateDirect(getBufferSize(javaSamplerContext) + 5).order(getByteOrder(javaSamplerContext));
        }
        return ByteBuffer.allocate(getBufferSize(javaSamplerContext) + 5).order(getByteOrder(javaSamplerContext));
    }

    private void putDefaultPacketOnBuffer(JavaSamplerContext javaSamplerContext) {
        short packetSize = getBufferSize(javaSamplerContext);
        sampleBuffer.putShort((short) (packetSize + 3));
        sampleBuffer.put((byte) 0x01);
        sampleBuffer.putShort(packetSize);

        while (packetSize >= 8) {
            sampleBuffer.putLong(Long.MAX_VALUE);
            packetSize -= 8;
        }

        while (packetSize >= 4) {
            sampleBuffer.putInt(Integer.MAX_VALUE);
            packetSize -= 4;
        }

        while (packetSize >= 2) {
            sampleBuffer.putShort(Short.MAX_VALUE);
            packetSize -= 2;
        }

        while (packetSize >= 1) {
            sampleBuffer.put(Byte.MAX_VALUE);
            packetSize--;
        }

        sampleBuffer.flip();
    }

    private void createSampleBuffer(JavaSamplerContext javaSamplerContext) {
        defaultPacket = true;
        int bufferSize = getBufferSize(javaSamplerContext);
        boolean sendDirect = isSendDirect(javaSamplerContext);

        ByteOrder order = getByteOrder(javaSamplerContext);

        if (sendDirect) {
            sampleBuffer = ByteBuffer.allocateDirect(bufferSize + 5).order(order);
        } else {
            sampleBuffer = ByteBuffer.allocate(bufferSize + 5).order(order);
        }
    }


    private short getBufferSize(JavaSamplerContext javaSamplerContext) {
        int bufferSize = javaSamplerContext.getIntParameter(BUFFER_SIZE);
        return bufferSize > Short.MAX_VALUE ?  Short.MAX_VALUE  : (short) bufferSize;
    }

    private boolean isSendDirect(JavaSamplerContext javaSamplerContext) {
        return "true".equalsIgnoreCase(javaSamplerContext.getParameter(SEND_DIRECT));
    }

    private ByteOrder getByteOrder(JavaSamplerContext javaSamplerContext) {
        String optByteOrder = javaSamplerContext.getParameter(BYTE_ORDER);
        ByteOrder order = ByteOrder.nativeOrder();

        if(optByteOrder.toLowerCase().startsWith("little")) {
            order = ByteOrder.LITTLE_ENDIAN;
        } else if (optByteOrder.toLowerCase().startsWith("big")) {
            order = ByteOrder.BIG_ENDIAN;
        }
        return order;
    }

    public void setSampleBuffer(ByteBuffer buffer) {
        sampleBuffer = buffer;
    }

    private ByteBuffer getSampleBuffer() {
        return sampleBuffer;
    }

    public ByteBuffer getResultBuffer() {
        return rBuffer;
    }

    @Override
    public void threadStarted() {

    }

    @Override
    public void threadFinished() {
    }
}
