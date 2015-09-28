package com.github.brosander.kettle.socket.input;

import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.ExecutorUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bryan on 9/11/15.
 */
public class SocketInput extends BaseStep implements StepInterface {
    public static final String CONTEXT_PATH = "/docker/runTrans";
    private final AtomicInteger openSockets;
    private final AtomicBoolean stopped;
    private ServerSocket serverSocket = null;

    public SocketInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
        openSockets = new AtomicInteger(0);
        stopped = new AtomicBoolean(false);
    }

    @Override
    public void stopRunning(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface) throws KettleException {
        stopped.set(true);
        try {
            serverSocket.close();
        } catch (IOException e) {
            // Ignore
        }
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        SocketInputMeta socketInputMeta = (SocketInputMeta) smi;
        if (first) {
            try {
                serverSocket = new ServerSocket(socketInputMeta.getPort(), 50, InetAddress.getByName(socketInputMeta.getBindAddress()));
                final ExecutorService executorService = ExecutorUtil.getExecutor();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (!stopped.get()) {
                            try {
                                final Socket accept = serverSocket.accept();
                                openSockets.incrementAndGet();
                                executorService.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        DataOutputStream dataOutputStream = null;
                                        try {
                                            DataInputStream dataInputStream = new DataInputStream(accept.getInputStream());
                                            dataOutputStream = new DataOutputStream(accept.getOutputStream());
                                            RowMetaInterface rowMetaInterface = null;
                                            boolean done = false;
                                            while (!done && !stopped.get()) {
                                                byte controlByte = dataInputStream.readByte();
                                                InputControlCode code = InputControlCode.getCode(controlByte);
                                                switch (code) {
                                                    // Stop step, fall through
                                                    case TERMINATE_STEP:
                                                        stopped.set(true);
                                                        serverSocket.close();
                                                    case STOP:
                                                        done = true;
                                                        break;
                                                    case ROW_META:
                                                        rowMetaInterface = new RowMeta(dataInputStream);
                                                        break;
                                                    case ROW:
                                                        putRow(rowMetaInterface, rowMetaInterface.readData(dataInputStream));
                                                        break;
                                                    default:
                                                        throw new RuntimeException("Unhandled control code: " + controlByte);
                                                }
                                            }
                                            dataOutputStream.writeByte(OutputControlCode.SUCCESS.ordinal());
                                            dataOutputStream.flush();
                                        } catch (Exception e) {
                                            logError(e.getMessage(), e);
                                            if (dataOutputStream != null) {
                                                try {
                                                    dataOutputStream.writeByte(OutputControlCode.ERROR.ordinal());
                                                    dataOutputStream.writeUTF(e.getMessage());
                                                    dataOutputStream.flush();
                                                } catch (IOException e1) {
                                                    // Ignore, won't be able to notify the writer of error
                                                }
                                            }
                                        } finally {
                                            openSockets.decrementAndGet();
                                            try {
                                                accept.close();
                                            } catch (IOException e) {
                                                // Ignore
                                            }
                                        }
                                    }
                                });
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            serverSocket.close();
                        } catch (IOException e) {
                            // Ignore
                        }
                    }
                }).start();
            } catch (IOException e) {
                throw new KettleException(e);
            }
            first = false;
        }
        return true;
    }

    public enum InputControlCode {
        STOP, ROW_META, ROW, TERMINATE_STEP;

        public static InputControlCode getCode(byte value) {
            return values()[value];
        }
    }

    public enum OutputControlCode {
        SUCCESS, ERROR
    }
}
