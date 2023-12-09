


package org.hsqldb.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;
import java.awt.Dimension;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.border.EmptyBorder;













class ConnectionDialogSwing extends JDialog
implements ActionListener, ItemListener {

    
    private static final long serialVersionUID = 1L;
    private Connection        mConnection;
    private JTextField        mName, mDriver, mURL, mUser;
    private JPasswordField    mPassword;
    private String[][]        connTypes;
    private Hashtable         settings;
    private JButton           okCancel, clear;
    private JComboBox mSettingName =
        new JComboBox(loadRecentConnectionSettings());
    private static ConnectionSetting currentConnectionSetting = null;

    public static void setConnectionSetting(
            ConnectionSetting connectionSetting) {
        currentConnectionSetting = connectionSetting;
    }

    public static Connection createConnection(String driver, String url,
            String user, String password) throws Exception {

        Class.forName(driver).newInstance();

        return DriverManager.getConnection(url, user, password);
    }

    ConnectionDialogSwing(JFrame owner, String title) {
        super(owner, title, true);
    }

    private void create() {

        Box main     = Box.createHorizontalBox();
        Box labels   = Box.createVerticalBox();
        Box controls = Box.createVerticalBox();
        Box buttons  = Box.createHorizontalBox();
        Box whole    = Box.createVerticalBox();

        
        Box extra = Box.createHorizontalBox();

        main.add(Box.createHorizontalStrut(10));
        main.add(Box.createHorizontalGlue());
        main.add(labels);
        main.add(Box.createHorizontalStrut(10));
        main.add(Box.createHorizontalGlue());
        main.add(controls);
        main.add(Box.createHorizontalStrut(10));
        main.add(Box.createVerticalGlue());
        main.add(extra);
        main.add(Box.createVerticalGlue());
        whole.add(Box.createVerticalGlue());
        whole.add(Box.createVerticalStrut(10));
        whole.add(main);
        whole.add(Box.createVerticalGlue());
        whole.add(Box.createVerticalStrut(10));
        whole.add(buttons);
        whole.add(Box.createVerticalGlue());
        whole.add(Box.createVerticalStrut(10));
        whole.add(Box.createVerticalGlue());
        labels.add(createLabel("Recent Setting:"));
        labels.add(Box.createVerticalGlue());
        labels.add(createLabel("Setting Name:"));
        labels.add(Box.createVerticalGlue());
        labels.add(createLabel("Type:"));
        labels.add(Box.createVerticalGlue());
        labels.add(createLabel("Driver:"));
        labels.add(Box.createVerticalGlue());
        labels.add(createLabel("URL:"));
        labels.add(Box.createVerticalGlue());
        labels.add(createLabel("User:"));
        labels.add(Box.createVerticalGlue());
        labels.add(createLabel("Password:"));
        labels.add(Box.createVerticalGlue());
        labels.add(Box.createVerticalStrut(10));
        controls.add(Box.createVerticalGlue());

        
        mSettingName.setActionCommand("Select Setting");
        mSettingName.addActionListener(this);
        controls.add(mSettingName);
        controls.add(Box.createHorizontalGlue());

        
        mName = new JTextField();

        mName.addActionListener(this);
        controls.add(mName);

        
        clear = new JButton("Clear Names");

        clear.setActionCommand("Clear");
        clear.addActionListener(this);
        buttons.add(clear);
        buttons.add(Box.createHorizontalGlue());
        buttons.add(Box.createHorizontalStrut(10));

        JComboBox types = new JComboBox();

        connTypes = ConnectionDialogCommon.getTypes();

        for (int i = 0; i < connTypes.length; i++) {
            types.addItem(connTypes[i][0]);
        }

        types.addItemListener(this);
        controls.add(types);
        controls.add(Box.createVerticalGlue());

        mDriver = new JTextField(connTypes[0][1]);

        mDriver.addActionListener(this);
        controls.add(mDriver);

        mURL = new JTextField(connTypes[0][2]);

        mURL.addActionListener(this);
        controls.add(mURL);
        controls.add(Box.createVerticalGlue());

        mUser = new JTextField("SA");

        mUser.addActionListener(this);
        controls.add(mUser);
        controls.add(Box.createVerticalGlue());

        mPassword = new JPasswordField("");

        mPassword.addActionListener(this);
        controls.add(mPassword);
        controls.add(Box.createVerticalGlue());
        controls.add(Box.createVerticalStrut(10));

        
        buttons.add(Box.createHorizontalGlue());
        buttons.add(Box.createHorizontalStrut(10));

        okCancel = new JButton("     Ok      ");

        okCancel.setActionCommand("ConnectOk");
        okCancel.addActionListener(this);
        buttons.add(okCancel);
        getRootPane().setDefaultButton(okCancel);
        buttons.add(Box.createHorizontalGlue());
        buttons.add(Box.createHorizontalStrut(20));

        okCancel = new JButton("  Cancel   ");

        okCancel.setActionCommand("ConnectCancel");
        okCancel.addActionListener(this);
        buttons.add(okCancel);
        buttons.add(Box.createHorizontalGlue());
        buttons.add(Box.createHorizontalStrut(10));

        JPanel jPanel = new JPanel();

        jPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        jPanel.add("Center", whole);
        getContentPane().add("Center", jPanel);
        doLayout();
        pack();

        Dimension d    = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = getSize();

        if (currentConnectionSetting != null) {
            mName.setText(currentConnectionSetting.getName());
            mDriver.setText(currentConnectionSetting.getDriver());
            mURL.setText(currentConnectionSetting.getUrl());
            mUser.setText(currentConnectionSetting.getUser());
            mPassword.setText(currentConnectionSetting.getPassword());
        }

        
        if (d.width >= 640) {
            setLocation((d.width - size.width) / 2,
                        (d.height - size.height) / 2);
        } else {
            setLocation(0, 0);
            setSize(d);
        }

        setVisible(true);
    }

    public static Connection createConnection(JFrame owner, String title) {

        ConnectionDialogSwing dialog = new ConnectionDialogSwing(owner,
            title);


        try {


            SwingUtilities.updateComponentTreeUI(dialog);
        } catch (Exception e) {
            CommonSwing.errorMessage(e);
        }

        dialog.create();

        return dialog.mConnection;
    }

    private static JLabel createLabel(String s) {

        JLabel l = new JLabel(s);

        return l;
    }

    
    public Vector loadRecentConnectionSettings() {

        Vector passSettings = new Vector();

        settings = new Hashtable();

        try {
            settings = ConnectionDialogCommon.loadRecentConnectionSettings();

            Iterator it = settings.values().iterator();

            passSettings.add(ConnectionDialogCommon.emptySettingName);

            while (it.hasNext()) {
                passSettings.add(((ConnectionSetting) it.next()).getName());
            }
        } catch (java.io.IOException ioe) {
            CommonSwing.errorMessage(ioe);
        }

        return (passSettings);
    }

    public void actionPerformed(ActionEvent ev) {

        String s = ev.getActionCommand();

        if (s.equals("ConnectOk") || (ev.getSource() instanceof JTextField)) {
            try {
                if (mURL.getText().indexOf('\u00AB') >= 0) {
                    throw new Exception("please specify db path");
                }

                mConnection =
                    createConnection(mDriver.getText(), mURL.getText(),
                                     mUser.getText(),
                                     new String(mPassword.getPassword()));

                
                if (mName.getText() != null
                        && mName.getText().trim().length() != 0) {
                    ConnectionSetting newSetting = new ConnectionSetting(
                        mName.getText(), mDriver.getText(), mURL.getText(),
                        mUser.getText(), new String(mPassword.getPassword()));

                    ConnectionDialogCommon.addToRecentConnectionSettings(
                        settings, newSetting);
                }

                dispose();
            } catch (SQLException e) {
                mConnection = null;

                CommonSwing.errorMessage(e, true);
            } catch (Exception e) {

                
                CommonSwing.errorMessage(e);
            }

            
        } else if (s.equals("Select Setting")) {
            String            s2 = (String) mSettingName.getSelectedItem();
            ConnectionSetting setting = (ConnectionSetting) settings.get(s2);

            if (setting != null) {
                mName.setText(setting.getName());
                mDriver.setText(setting.getDriver());
                mURL.setText(setting.getUrl());
                mUser.setText(setting.getUser());
                mPassword.setText(setting.getPassword());
            }
        } else if (s.equals("ConnectCancel")) {
            dispose();

            
        } else if (s.equals("Clear")) {
            ConnectionDialogCommon.deleteRecentConnectionSettings();

            settings = new Hashtable();

            mSettingName.removeAllItems();
            mSettingName.addItem(ConnectionDialogCommon.emptySettingName);
            mName.setText(null);
        }
    }

    public void itemStateChanged(ItemEvent e) {

        String s = (String) e.getItem();

        for (int i = 0; i < connTypes.length; i++) {
            if (s.equals(connTypes[i][0])) {
                mDriver.setText(connTypes[i][1]);
                mURL.setText(connTypes[i][2]);
            }
        }
    }
}
