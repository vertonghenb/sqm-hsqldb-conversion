package org.hsqldb.util;
import java.awt.Dimension;
import java.awt.Image;
import java.awt.Toolkit;
import javax.swing.ImageIcon;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
class CommonSwing {
    protected static String messagerHeader = "Database Manager Swing Error";
    protected static String Native         = "Native";
    protected static String Java           = "Java";
    protected static String Motif          = "Motif";
    protected static String plaf           = "plaf";
    protected static String GTK            = "GTK";
    static Image getIcon(String target) {
        if (target.equalsIgnoreCase("SystemCursor")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Hourglass.gif")).getImage());
        } else if (target.equalsIgnoreCase("Frame")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("hsqldb.gif")).getImage());
        } else if (target.equalsIgnoreCase("Execute")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("run_exc.gif")).getImage());
        } else if (target.equalsIgnoreCase("StatusRunning")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("RedCircle.gif")).getImage());
        } else if (target.equalsIgnoreCase("StatusReady")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("GreenCircle.gif")).getImage());
        } else if (target.equalsIgnoreCase("Clear")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Clear.png")).getImage());
        } else if (target.equalsIgnoreCase("Problem")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("problems.gif")).getImage());
        } else if (target.equalsIgnoreCase("BoldFont")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Bold.gif")).getImage());
        } else if (target.equalsIgnoreCase("ItalicFont")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Italic.gif")).getImage());
        } else if (target.equalsIgnoreCase("ColorSelection")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Colors.png")).getImage());
        } else if (target.equalsIgnoreCase("Close")) {
            return (new ImageIcon(
                CommonSwing.class.getResource("Close.png")).getImage());
        } else {
            return (null);
        }
    }
    protected static void errorMessage(String errorMessage) {
        Object[] options = { "OK" };
        JOptionPane.showOptionDialog(null, errorMessage, messagerHeader,
                                     JOptionPane.DEFAULT_OPTION,
                                     JOptionPane.WARNING_MESSAGE, null,
                                     options, options[0]);
    }
    public static void errorMessage(Exception exceptionMsg) {
        errorMessage(exceptionMsg, false);
    }
    public static void errorMessage(Exception exceptionMsg, boolean quiet) {
        Object[] options = { "OK", };
        JOptionPane.showOptionDialog(null, exceptionMsg, messagerHeader,
                                     JOptionPane.DEFAULT_OPTION,
                                     JOptionPane.ERROR_MESSAGE, null,
                                     options, options[0]);
        if (!quiet) {
            exceptionMsg.printStackTrace();
        }
    }
    static void setFramePositon(JFrame inTargetFrame) {
        Dimension d    = Toolkit.getDefaultToolkit().getScreenSize();
        Dimension size = inTargetFrame.getSize();
        if (d.width >= 640) {
            inTargetFrame.setLocation((d.width - size.width) / 2,
                                      (d.height - size.height) / 2);
        } else {
            inTargetFrame.setLocation(0, 0);
            inTargetFrame.setSize(d);
        }
    }
    static void setSwingLAF(java.awt.Component comp, String targetTheme) {
        try {
            if (targetTheme.equalsIgnoreCase(Native)) {
                UIManager.setLookAndFeel(
                    UIManager.getSystemLookAndFeelClassName());
            } else if (targetTheme.equalsIgnoreCase(Java)) {
                UIManager.setLookAndFeel(
                    UIManager.getCrossPlatformLookAndFeelClassName());
            } else if (targetTheme.equalsIgnoreCase(Motif)) {
                UIManager.setLookAndFeel(
                    "com.sun.java.swing.plaf.motif.MotifLookAndFeel");
            }
            SwingUtilities.updateComponentTreeUI(comp);
            if (comp instanceof java.awt.Frame) {
                ((java.awt.Frame) comp).pack();
            }
        } catch (Exception e) {
            errorMessage(e);
        }
    }
}