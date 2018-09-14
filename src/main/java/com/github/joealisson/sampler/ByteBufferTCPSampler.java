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

        if(isNull(sampleBuffer)) {

            createSampleBuffer(javaSamplerContext);
            putDefaultPacketOnBuffer();
        }

        rBuffer = getReadBuffer(javaSamplerContext);

        SampleResult result = new SampleResult();
        result.sampleStart();

        try {
            client.connect(server, port);
            client.send(getSampleBuffer());
            client.read(rBuffer);

            if(defaultPacket) {
                StringBuilder builder = makeDefaultResponse(result);
                result.setResponseMessage(builder.toString());
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
        }

        return result;
    }

    private StringBuilder makeDefaultResponse(SampleResult result) {
        rBuffer.flip();
        long send = rBuffer.getLong();
        long received = rBuffer.getLong();
        result.sampleEnd();
        result.setSuccessful(true);
        result.setResponseCodeOK();
        StringBuilder builder = new StringBuilder();
        builder.append("Initial Time: ");
        builder.append(Instant.ofEpochMilli(send).atZone(ZoneId.systemDefault()));
        builder.append("\n");
        builder.append("Server Received Time: ");
        builder.append(Instant.ofEpochMilli(received).atZone(ZoneId.systemDefault()));
        builder.append("\n");
        builder.append("Send Time: " );
        builder.append(received - send);
        builder.append("\n");
        builder.append("Round Trip Time: ");
        builder.append(result.getEndTime() - send);
        builder.append("\n");
        return builder;
    }

    private ByteBuffer getReadBuffer(JavaSamplerContext javaSamplerContext) {
        if(isSendDirect(javaSamplerContext)) {
            return ByteBuffer.allocateDirect(getBufferSize(javaSamplerContext)).order(getByteOrder(javaSamplerContext));
        }
        return ByteBuffer.allocate(getBufferSize(javaSamplerContext)).order(getByteOrder(javaSamplerContext));
    }

    private void putDefaultPacketOnBuffer() {
        sampleBuffer.putShort((short)11);
        sampleBuffer.put((byte) 0x01);
        sampleBuffer.putLong(System.currentTimeMillis());
        sampleBuffer.flip();
    }

    private void createSampleBuffer(JavaSamplerContext javaSamplerContext) {
        defaultPacket = true;
        int bufferSize = getBufferSize(javaSamplerContext);
        boolean sendDirect = isSendDirect(javaSamplerContext);

        ByteOrder order = getByteOrder(javaSamplerContext);

        if (sendDirect) {
            sampleBuffer = ByteBuffer.allocateDirect(bufferSize).order(order);
        } else {
            sampleBuffer = ByteBuffer.allocate(bufferSize).order(order);
        }
    }


    private int getBufferSize(JavaSamplerContext javaSamplerContext) {
        return javaSamplerContext.getIntParameter(BUFFER_SIZE);
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

    public ByteBuffer getSampleBuffer() {
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
        client.close();
    }
}
