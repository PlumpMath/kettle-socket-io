package com.github.brosander.kettle.socket.input;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Created by bryan on 9/10/15.
 */
@Step(id = "SocketInput", image = "ui/images/publish.svg", i18nPackageName = "com.github.brosander.kettle.socket.input", name = "SocketInputMeta.TransName", description = "SocketInputMeta.TransDescription", categoryDescription = "SocketInputMeta.CategoryDescription", classLoaderGroup = "SOCKET_IO")
public class SocketInputMeta extends BaseStepMeta implements StepMetaInterface {
    public static final String DEFAULT_BIND_ADDRESS = "0.0.0.0";
    public static final int DEFAULT_PORT = 9001;
    public static final String BIND_ADDRESS = "bindAddress";
    public static final String PORT = "port";
    private String bindAddress = DEFAULT_BIND_ADDRESS;
    private int port = DEFAULT_PORT;

    @Override
    public void setDefault() {
        bindAddress = DEFAULT_BIND_ADDRESS;
        port = DEFAULT_PORT;
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        return new SocketInput(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public StepDataInterface getStepData() {
        return new SocketInputData();
    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
        /*if (this.inputRowMeta == null) {
            throw new KettleStepException("Need to do a get fields from parent trans first");
        }
        inputRowMeta.clear();
        inputRowMeta.setValueMetaList(this.inputRowMeta.getValueMetaList());*/
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("    ").append(XMLHandler.addTagValue(BIND_ADDRESS, bindAddress));
        stringBuilder.append("    ").append(XMLHandler.addTagValue(PORT, port));
        return stringBuilder.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        bindAddress = XMLHandler.getTagValue(stepnode, BIND_ADDRESS);
        port = Integer.parseInt(XMLHandler.getTagValue(stepnode, PORT));
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        rep.saveStepAttribute(id_transformation, id_step, BIND_ADDRESS, bindAddress);
        rep.saveStepAttribute(id_transformation, id_step, PORT, port);
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases) throws KettleException {
        bindAddress = rep.getStepAttributeString(id_step, BIND_ADDRESS);
        port = (int) rep.getStepAttributeInteger(id_step, PORT);
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String getDialogClassName() {
        return SocketInputDialog.class.getCanonicalName();
    }
}
