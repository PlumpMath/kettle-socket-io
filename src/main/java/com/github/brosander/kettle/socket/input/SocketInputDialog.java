package com.github.brosander.kettle.socket.input;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.ui.trans.step.BaseStepXulDialog;
import org.pentaho.ui.xul.binding.Binding;

/**
 * Created by bryan on 9/10/15.
 */
public class SocketInputDialog extends BaseStepXulDialog {
    private String tempStepName;
    private String bindAddress;
    private String port;

    public SocketInputDialog(Shell parent, Object in, TransMeta transMeta, String stepname) {
        super("com/github/brosander/kettle/socket/input/socketInputDialog.xul", parent, (BaseStepMeta) in, transMeta, stepname);
        loadMeta((SocketInputMeta) baseStepMeta);
        this.stepname = stepname;
        tempStepName = stepname;
        this.transMeta = transMeta;
        try {
            bf.setBindingType(Binding.Type.BI_DIRECTIONAL);
            bf.createBinding(this, "tempStepName", "step-name", "value").fireSourceChanged();
            bf.createBinding(this, SocketInputMeta.BIND_ADDRESS, "bindAddress", "value").fireSourceChanged();
            bf.createBinding(this, SocketInputMeta.PORT, "port", "value").fireSourceChanged();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onAccept() {
        if (Const.isEmpty(tempStepName)) {
            return;
        }
        if (!stepname.equals(tempStepName)) {
            baseStepMeta.setChanged();
        }
        saveMeta((SocketInputMeta) baseStepMeta);
        dispose();
    }

    @Override
    public void onCancel() {
        dispose();
    }

    @Override
    protected Class<?> getClassForMessages() {
        return SocketInputDialog.class;
    }

    private boolean nullSafeEquals(String first, String second) {
        if (first == null) {
            if (second == null) {
                return true;
            }
            return false;
        }
        return first.equals(second);
    }

    public void saveMeta(SocketInputMeta meta) {
        if (!nullSafeEquals(meta.getBindAddress(), bindAddress) || !nullSafeEquals(meta.getPort() + "", port)) {
            baseStepMeta.setChanged();
        }
        meta.setBindAddress(bindAddress);
        meta.setPort(Integer.parseInt(port));
    }

    public void loadMeta(SocketInputMeta meta) {
        bindAddress = meta.getBindAddress();
        port = meta.getPort() + "";
    }

    public String getTempStepName() {
        return tempStepName;
    }

    public void setTempStepName(String tempStepName) {
        this.tempStepName = tempStepName;
    }

    public String getBindAddress() {
        return bindAddress;
    }

    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
