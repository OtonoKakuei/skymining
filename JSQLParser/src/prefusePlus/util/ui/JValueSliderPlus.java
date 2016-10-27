package prefusePlus.util.ui;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JSlider;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.EventListenerList;
import prefuse.util.ui.JFastLabel;

/**
 * A new Swing component that contains a negative label (-), a slider, a positive
 * label (+), to allow users increase and decrease a range, e.g. zoom in or out,
 * adjust values.<p/>
 * Partially use prfuse.util.ui.JValueSlider codes.<p/>
 *
 * @author Jian Zhang from Drexel IST
 */
@SuppressWarnings("serial")
public class JValueSliderPlus extends JComponent {

    public static final int HORIZONTAL = 0;
    public static final int VERTICAL = 1;

    private Number z_min, z_max, z_value, z_unit = 1;
    private boolean z_ignore = false;
    private int z_orientation = HORIZONTAL;

    private JFastLabel z_title;
    private JButton z_negativeB,
                    z_positiveB;
    private JSlider z_slider;

    private EventListenerList z_listeners;

    private int z_smin = 0;
    private int z_srang = 100;

    /**
     * Construct a new ValueSliderPlus with title, minimum, maximum, increase unit,
     * current value, and orientation.<p/>
     * @param title - title label of this slider<p/>
     * @param min - minimum value<p/>
     * @param max - maximum value<p/>
     * @param unit - increment interval<p/>
     * @param value - initial value<p/>
     * @param orientation - horizontal or vertical<p/>
     */
    public JValueSliderPlus (String title, int min, int max, int unit, int value, int orientation){
        this(title, new Integer(min), new Integer(max), new Integer(unit), new Integer(value), orientation);
        z_smin = min;
        z_srang = max-min;
        z_slider.setMinimum(min);
        z_slider.setMaximum(max);
        setValue(new Integer(value));
    }

    /**
     * Construct a Value Slider Plus with with title, minimum, maximum, increase unit,
     * current value, and orientation.<p/>
     * But the value of min, max, unit, and value should be a Number object.
     * @param title - title label of this slider<p/>
     * @param min - minimum value<p/>
     * @param max - maximum value<p/>
     * @param unit - increment interval<p/>
     * @param value - initial value<p/>
     * @param orientation - horizontal or vertical<p/>
     */
    public JValueSliderPlus(String title, Number min, Number max, Number unit, Number value, int orientation){
        z_min = min;
        z_max = max;
        z_value = value;
        z_unit = unit;
        z_orientation = orientation;

        z_title = new JFastLabel(title);
        z_negativeB = new JButton("-");
        z_positiveB = new JButton("+");
        z_slider = new JSlider();
        z_listeners = new EventListenerList();

        setValue(z_value);

        initUI();
    }

    /**
     * Protected method to set slider's value.<p/>
     */
    protected void setSliderValue(){
        int val;
        if ( z_value instanceof Double || z_value instanceof Float){
            double value = z_value.doubleValue();
            double min = z_min.doubleValue();
            double max = z_max.doubleValue();
            val = z_smin + (int) Math.round(z_srang * (value - min)/(max - min));
        } else {
            long value = z_value.longValue();
            long min = z_min.longValue();
            long max = z_max.longValue();
            val = z_smin + (int) (z_srang*(value - min)/(max - min));
        }
        z_slider.setValue(val);
    }

    /**
     * Protected method to get current slider value.<p/>
     * @return a Number object.
     */
    protected Number getSliderValue(){
        if (z_value instanceof Integer){
            int value = z_slider.getValue();
            int min = z_min.intValue();
            int max = z_max.intValue();
            return new Integer(min + (z_srang*(value-min))/(max-min));
        } else {
            if ( z_value instanceof Long){
                int value = z_slider.getValue();
                long min = z_min.longValue();
                long max = z_max.longValue();
                return new Long(min + (z_srang*(value-min))/(max-min));
            } else {
                double f = (z_slider.getValue() - z_smin)/(double) z_srang;
                double min = z_min.doubleValue();
                double max = z_max.doubleValue();
                double value = min + f*(max - min);
                return ( z_value instanceof Double ? (Number) new Double(value) :
                                                      new Float((float)value));
            }
        } //end if
    }

    /**
     * Add change listeners and action listeners of each component for interactions
     * and intial UI.<p/>
     */
    protected void initUI(){
        z_slider.addChangeListener(new ChangeListener(){
            public void stateChanged(ChangeEvent e) {
                if (z_ignore) return;
                z_ignore = true;
                z_value = getSliderValue();
                fireChangeEvent();
                z_ignore = false;
            }
        });

        z_negativeB.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e) {
                if (z_value instanceof Double || z_value instanceof Float){
                    double value = z_value.doubleValue();
                    double min = z_min.doubleValue();
                    double unit = z_unit.doubleValue();
                    if ( (value-unit)>min){
                        setValue(new Double(value-unit));
                    } else
                        setValue(new Double(min));
                } else {
                    if (z_value instanceof Long){
                        long value = z_value.longValue();
                        long min = z_min.longValue();
                        long unit = z_unit.longValue();
                        if ((value-unit)>min){
                            setValue(new Long(value-unit));
                        } else
                            setValue(new Long(min));
                    } else {
                        int value = z_value.intValue();
                        int min = z_min.intValue();
                        int unit = z_unit.intValue();
                        if ((value-unit)>min){
                            setValue(new Integer(value-unit));
                        } else
                            setValue(new Integer(min));
                    }
                }
            } //end action performed
        });

        z_positiveB.addActionListener(new ActionListener(){
            public void actionPerformed(ActionEvent e) {
                if (z_value instanceof Double || z_value instanceof Float){
                    double value = z_value.doubleValue();
                    double max = z_max.doubleValue();
                    double unit = z_unit.doubleValue();
                    if ( (value+unit)<max){
                        setValue(new Double(value+unit));
                    } else
                        setValue(new Double(max));
                } else {
                    if (z_value instanceof Long){
                        long value = z_value.longValue();
                        long max = z_max.longValue();
                        long unit = z_unit.longValue();
                        if ((value+unit)<max){
                            setValue(new Long(value+unit));
                        } else
                            setValue(new Long(max));
                    } else {
                        int value = z_value.intValue();
                        int max = z_max.intValue();
                        int unit = z_unit.intValue();
                        if ((value+unit)<max){
                            setValue(new Integer(value+unit));
                        } else
                            setValue(new Integer(max));
                    }
                }
            } //end action performed
        });

        z_negativeB.setMaximumSize(new Dimension(15, 20));
        z_positiveB.setMaximumSize(new Dimension(15, 20));

        switch (z_orientation){
            case VERTICAL: this.setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
                           this.add(z_title);
                           this.add(Box.createVerticalStrut(1));
                           this.add(z_positiveB);
                           this.add(z_slider);
                           this.add(z_negativeB);
                           break;
            default: this.setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
                     this.add(z_title);
                     this.add(Box.createHorizontalStrut(1));
                     this.add(z_negativeB);
                     this.add(z_slider);
                     this.add(z_positiveB);
                     break;
        } //end of switch
    } //end of intiUI

    /**
     * Set the current slider value.<p/>
     * @param value 
     */
    public void setValue(Number value){
        z_value = value;
        setSliderValue();
    }

    /**
     * Get the current slider value.<p/>
     * @return 
     */
    public Number getValue(){
        return z_value;
    }

    /**
     * Add action listener.<p/>
     * @param ce - a change listener.<p/>
     */
    public void addChangeListener(ChangeListener ce){
        z_listeners.add(ChangeListener.class, ce);
    }

    /**
     * Remove change listener<p/>
     * @param ce - a change listener<p/>
     */
    public void removeChangeListener(ChangeListener ce){
        z_listeners.remove(ChangeListener.class, ce);
    }

    private void fireChangeEvent(){
        Object[] listeners = z_listeners.getListenerList();

        int numlistener = listeners.length;
        ChangeEvent ce = new ChangeEvent(this);
        for (int i=0;i<numlistener;i+=2){
            if (listeners[i] == ChangeListener.class){
                ((ChangeListener)listeners[i+1]).stateChanged(ce);
            }
        }
    }

    @Override
    /**
     * set background colors of this slider plus.<p/>
     */
    public void setBackground(Color c){
        z_title.setBackground(c);
        z_positiveB.setBackground(c);
        z_slider.setBackground(c);
        z_negativeB.setBackground(c);
        super.setBackground(c);
    }

    @Override
    /**
     * Set foreground colors of this slider plus.<p/>
     */
    public void setForeground(Color c){
        z_title.setForeground(c);
        z_positiveB.setForeground(c);
        z_slider.setForeground(c);
        z_negativeB.setForeground(c);
        super.setForeground(c);
    }

}
