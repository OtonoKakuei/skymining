package prefusePlus.util.ui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import prefuse.data.query.NumberRangeModel;
import prefuse.util.StringLib;
import prefuse.util.ui.JFastLabel;
import prefuse.util.ui.JRangeSlider;

/**
 * <p>Enhance Jeffery Heer's prefuse.util.ui.JRangeSlider. </p>
 * <p>Add labels that show the max and lowValue. and current lower and higher values.
 * Allow users to input lower and higher value.</p>
 * <p>Auto check if the lower value indeed is lower than the higher value.</p>
 * 
 * <p>Current version only support horizontal layout</p>
 *
 * @author James Zhang
 */
@SuppressWarnings("serial")
public class JRangeSliderPlus extends JComponent{

    private JRangeSlider z_slider;
    private JTextField z_lowField = new JTextField(8),
                       z_highField = new JTextField(8);
    
    private NumberRangeModel z_model;
    private Number z_minValue, z_maxValue, z_lowValue, z_highValue;
    private int z_slowValue, z_shighValue;
    
    private boolean z_ignore = false;
    private boolean z_isFormatRight = false;
    private boolean z_isFieldAction = false;
    private boolean z_isStepTooSmall = false;

    private ChangeEvent z_event = null;
    @SuppressWarnings("rawtypes")
	private ArrayList z_listeners;

    private int z_smin = 0;
    private int z_srange =10000;

    /**
     * Create a new JRangeSliderPlus
     * @param lowValue - low value in the range slider, and appear in low value text field
     * @param highValue - high value in the range slider, and appear in high value text field
     * @param minimum - minimum value associated with the slider, and appear in min value label
     * @param maximum - maximum value associated with the slider, and appear in max value label
     */
    public JRangeSliderPlus(int lowValue, int highValue, int minimum, int maximum){
        this(new NumberRangeModel(lowValue, highValue, minimum, maximum));
    }

    /**
     * Create a new JRangeSliderPlus
     * @param lowValue - low value in the range slider, and appear in low value text field
     * @param highValue - high value in the range slider, and appear in high value text field
     * @param minimum - minimum value associated with the slider, and appear in min value label
     * @param maximum - maximum value associated with the slider, and appear in max value label
     */
    public JRangeSliderPlus(long lowValue, long highValue, long minimum, long maximum){
        this(new NumberRangeModel(lowValue, highValue, minimum, maximum));
    }

    /**
     * Create a new JRangeSliderPlus
     * @param lowValue - low value in the range slider, and appear in low value text field
     * @param highValue - high value in the range slider, and appear in high value text field
     * @param minimum - minimum value associated with the slider, and appear in min value label
     * @param maximum - maximum value associated with the slider, and appear in max value label
     */
    public JRangeSliderPlus(float lowValue, float highValue, float minimum, float maximum){
        this(new NumberRangeModel(lowValue, highValue, minimum, maximum));
    }

    /**
     * Create a new JRangeSliderPlus
     * @param lowValue - low value in the range slider, and appear in low value text field
     * @param highValue - high value in the range slider, and appear in high value text field
     * @param minimum - minimum value associated with the slider, and appear in min value label
     * @param maximum - maximum value associated with the slider, and appear in max value label
     */
    public JRangeSliderPlus(double lowValue, double highValue, double minimum, double maximum){
        this(new NumberRangeModel(lowValue, highValue, minimum, maximum));
    }
    
    /**
     * Create a new range slider plus.
     * @param model - a NumberRangeModel specifying the slider number range
     */
    @SuppressWarnings("rawtypes")
	public JRangeSliderPlus(NumberRangeModel model){
        z_model = model;
        z_slider = new JRangeSlider(z_model,
                JRangeSlider.HORIZONTAL, JRangeSlider.LEFTRIGHT_TOPBOTTOM);
        z_minValue = (Number) z_model.getMinValue();
        z_maxValue = (Number) z_model.getMaxValue();
        z_lowValue = (Number) z_model.getLowValue();
        z_highValue = (Number) z_model.getHighValue();

        z_listeners = new ArrayList();

        setLowFieldValue();
        setHighFieldValue();
        setSliderHighValue();

        z_slowValue = z_slider.getLowValue();
        z_shighValue = z_slider.getHighValue();

        initUI();
    }
    
    private void initUI(){
    
        //--1. create other components in this slider
        JFastLabel minLabel = new JFastLabel("Minimum: " + String.valueOf(z_model.getMinValue())),
                   maxLabel = new JFastLabel("Maximum: " + String.valueOf(z_model.getMaxValue()));

        z_lowField.setText(String.valueOf(z_lowValue));
        z_highField.setText(String.valueOf(z_highValue));

        minLabel.setPreferredSize(new Dimension(100, 15));
        maxLabel.setPreferredSize(new Dimension(100, 15));

        z_slider.setThumbColor(null);

        JPanel topPanel = new JPanel();
        JPanel bottomPanel = new JPanel();

        topPanel.setLayout(new BorderLayout());
        topPanel.add(minLabel, BorderLayout.WEST);
        topPanel.add(new JLabel("  "), BorderLayout.CENTER);
        topPanel.add(maxLabel, BorderLayout.EAST);

        bottomPanel.setLayout(new BorderLayout());
        bottomPanel.add(z_lowField, BorderLayout.WEST);
        bottomPanel.add(new JLabel("  "), BorderLayout.CENTER);
        bottomPanel.add(z_highField, BorderLayout.EAST);

        this.setLayout(new GridLayout(3,1));
        this.add(topPanel);
        this.add(z_slider);
        this.add(bottomPanel);

        this.setPreferredSize(new Dimension(200, 50));

        //--2. add action listeners
        z_lowField.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
                //-- set control booleans
                if (z_ignore) return;
                z_ignore = true;
                z_isFormatRight = true;
                z_isFieldAction = true;
                //-- get input number and set this as the low value
                z_lowValue = getLowFieldValue();

                //-- check if format right. Yes: set slider and fire an event
                if (z_isFormatRight){
                    z_ignore = false;
                    z_isFormatRight = false;
                    setSliderLowValue();
                    //-- check if step is too small to trigger slider to fire an event
                    if (z_isStepTooSmall){      //too small, fire an event by itself
                        z_isStepTooSmall = false;
                        fireChangeEvent();
                    }
                } else {                  //No: reset to old value and do nothing
                    setLowFieldValue();
                    z_ignore = false;
                }

            }  //end actionperformed
        });

        z_highField.addActionListener(new ActionListener(){
            @Override
            public void actionPerformed(ActionEvent e) {
                //-- set control booleans
                if (z_ignore) return;
                z_ignore = true;
                z_isFormatRight = true;
                z_isFieldAction = true;
                //-- get input number and set this as the low value
                z_highValue = getHighFieldValue();

                //-- check if format right. Yes: set slider and fire an event
                if (z_isFormatRight){
                    z_ignore = false;
                    z_isFormatRight = false;
                    setSliderHighValue();
                    //-- check if step is too small to trigger slider to fire an event
                    if (z_isStepTooSmall){      //too small, fire an event by itself
                        z_isStepTooSmall = false;
                        fireChangeEvent();
                    }
                } else {
                    setHighFieldValue();   //No: reset to old value and do nothing
                    z_ignore = false;
                }
            }
        });

        z_slider.addChangeListener(new ChangeListener(){
            @Override
            public void stateChanged(ChangeEvent e) {
                if (z_ignore) return;
                z_ignore = true;

                //-- check if this changeaction is triggered by text field or slider
                if (!z_isFieldAction) {         //triggered by slider
        
                    //-- check which button was changed
                    if (z_slider.getLowValue() != z_slowValue){
                        z_slowValue = z_slider.getLowValue();
                        z_lowValue = getSliderLowValue();
                        setLowFieldValue();
                    }
                    if (z_slider.getHighValue() != z_shighValue){
                        z_shighValue = z_slider.getHighValue();
                        z_highValue = getSliderHighValue();
                        setHighFieldValue();
                    }
                } else {                        //triggered by text fields
                    z_isFieldAction = false;    //not reset slider value just fireevent
                }

                fireChangeEvent();

                z_ignore = false;
            }
        });
    }

    /**
     * <p>Get the low value form the low value text field. </p>
     * <p>Parse the input text.</p>
     * <p>if input is not a number, return current low value and set formate right
     * = false.</p>
     * <p>Else, convert the text to a number, and check if this number < min or
     * > high value. if yes, return current low value and set formate right
     * = false.</p>
     * <p>If all right, return the number</p>
     * @return the input low value in Number format.
     */
    private Number getLowFieldValue(){
        
        String min_string = z_lowField.getText();
        
        if ( (z_lowValue instanceof Double) || (z_lowValue instanceof Float) ){
            double lv;
            try{
                lv = Double.parseDouble(min_string);
            } catch (Exception e){
                z_isFormatRight = false;
                return z_lowValue;
            }
            if (lv < z_minValue.doubleValue() || lv > z_highValue.doubleValue()){
                z_isFormatRight = false;
                return z_lowValue;
            }
            return (z_lowValue instanceof Double ? (Number) new Double(lv):
                                                    new Float((float)lv));
        } else {
            long lv;
            try{
                lv = Long.parseLong(min_string);
            } catch (Exception e){
                z_isFormatRight = false;
                return z_lowValue;
            }
            if (lv < z_minValue.longValue() || lv > z_highValue.longValue()){
                z_isFormatRight = false;
                return z_lowValue;
            }
            return (z_lowValue instanceof Long ? (Number) new Long(lv):
                                                  new Integer((int)lv));
        }
    }

    /**
     * Set the low value field with the current low value.
     */
    private void setLowFieldValue(){
        String text;
        if (z_lowValue instanceof Double || z_lowValue instanceof Float){
            text = StringLib.formatNumber(z_lowValue.doubleValue(), 3);
        } else {
            text = String.valueOf(z_lowValue.longValue());
        }
        z_lowField.setText(text);
    }

    /**
     * <p>Get the high value form the high value text field. </p>
     * <p>Parse the input text.</p>
     * <p>if input is not a number, return current high value and set formate right
     * = false.</p>
     * <p>Else, convert the text to a number, and check if this number > max or
     * < low value. if yes, return current low value and set formate right
     * = false.</p>
     * <p>If all right, return the number</p>
     * @return the input high value in Number format.
     */
    private Number getHighFieldValue(){

        String min_string = z_highField.getText();

        if ( (z_highValue instanceof Double) || (z_highValue instanceof Float) ){
            double hv;
            try{
                hv = Double.parseDouble(min_string);
            } catch (Exception e){
                z_isFormatRight = false;
                return z_highValue;
            }
            if (hv > z_maxValue.doubleValue() || hv < z_lowValue.doubleValue()){
                z_isFormatRight = false;
                return z_highValue;
            }
            return (z_highValue instanceof Double ? (Number) new Double(hv):
                                                     new Float((float)hv));
        } else {
            long hv;
            try{
                hv = Long.parseLong(min_string);
            } catch (Exception e){
                z_isFormatRight = false;
                return z_highValue;
            }
            if (hv < z_lowValue.longValue() || hv > z_maxValue.longValue()){
                z_isFormatRight = false;
                return z_highValue;
            }
            return (z_highValue instanceof Long ? (Number) new Long(hv):
                                                   new Integer((int)hv));
        }
    }

    /**
     * Set the high value field with the current high value.
     */
    private void setHighFieldValue(){
        String text;
        if (z_highValue instanceof Double || z_highValue instanceof Float){
            text = StringLib.formatNumber(z_highValue.doubleValue(), 3);
        } else {
            text = String.valueOf(z_highValue.longValue());
        }
        z_highField.setText(text);
    }

    /**
     * Compute the current low value from the current slider low position
     * @return the current low value in Number format
     */
    private Number getSliderLowValue(){

        if (z_lowValue instanceof Integer){
            int slv = z_slider.getLowValue();
            int min = z_minValue.intValue();
            int max = z_maxValue.intValue();
            return new Integer(min + ((slv - z_smin)*(max - min))/z_srange);
        } else {
            if (z_lowValue instanceof Long){
                int slv = z_slider.getLowValue();
                long min = z_minValue.longValue();
                long max = z_maxValue.longValue();
                return new Long(min + ((slv - z_smin)*(max - min))/z_srange);
            } else {
                int slv = z_slider.getLowValue();
                double min = z_minValue.doubleValue();
                double max = z_maxValue.doubleValue();
                double f = (slv - z_smin)/((double)z_srange);
                double lv = min + f*(max - min);
                return (z_lowValue instanceof Double ? (Number) new Double(lv):
                                                        new Float((float) lv));
            }
        }  // end of if
    }

    /**
     * <p>Set the slider low position with the current low value.</p>
     * <p>if the current value is too small to make the slider change, which will
     * not trigger the range slider to fire a ChangeEvent, set isStepTooSmall
     * =  true, so that the text field know it should fire a ChangeEvent</p>
     */
    private void setSliderLowValue(){
        //--1. get the previous slider value in int
        int pv = z_slider.getLowValue();
        //--2. compute the new slider value in int
        int slv;
        if (z_lowValue instanceof Integer || z_lowValue instanceof Long ){
            long min = z_minValue.intValue();
            long max = z_maxValue.intValue();
            long lv = z_lowValue.longValue();
            slv = z_smin + (int)(((lv - min) * z_srange)/(max - min));
        } else {
            double min = z_minValue.doubleValue();
            double max = z_maxValue.doubleValue();
            double lv = z_lowValue.doubleValue();
            slv = z_smin + (int)Math.round((z_srange * (lv - min))/(max - min));
        }
        //--3. check if the new value == the previous value, and set the control
        if (slv == pv){
            z_isStepTooSmall = true;
        }
        //--4. reset the slider value in int.
        z_slider.setLowValue(slv);
    }

    /**
     * Compute the current high value from the current slider low position
     * @return the current high value in Number format
     */
    private Number getSliderHighValue(){

        if (z_highValue instanceof Integer){
            int shv = z_slider.getHighValue();
            int min = z_minValue.intValue();
            int max = z_maxValue.intValue();
            return new Integer(min + ((shv - z_smin)*(max - min))/z_srange);
        } else {
            if (z_highValue instanceof Long){
                int shv = z_slider.getHighValue();
                long min = z_minValue.longValue();
                long max = z_maxValue.longValue();
                return new Long(min + ((shv - z_smin)*(max - min))/z_srange);
            } else {
                int shv = z_slider.getHighValue();
                double min = z_minValue.doubleValue();
                double max = z_maxValue.doubleValue();
                double f = (shv - z_smin)/((double)z_srange);
                double lv = min + f*(max - min);
                return (z_highValue instanceof Double ? (Number) new Double(lv):
                                                        new Float((float) lv));
            }
        }  // end of if

    }

    /**
     * <p>Set the slider high position with the current high value.</p>
     * <p>if the current value is too small to make the slider change, which will
     * not trigger the range slider to fire a ChangeEvent, set isStepTooSmall
     * =  true, so that the text field know it should fire a ChangeEvent</p>
     */
    private void setSliderHighValue(){
        //--1. get the previous slider value in int
        int pv = z_slider.getHighValue();
        //--2. compute the new slider value in int
        int shv;
        if (z_highValue instanceof Integer || z_highValue instanceof Long ){
            long min = z_minValue.intValue();
            long max = z_maxValue.intValue();
            long hv = z_highValue.longValue();
            shv = z_smin + (int)(((hv - min) * z_srange)/(max - min));
        } else {
            double min = z_minValue.doubleValue();
            double max = z_maxValue.doubleValue();
            double hv = z_highValue.doubleValue();
            shv = z_smin + (int)Math.round((z_srange * (hv - min))/(max - min));
        }
        //--3. check if the new value == the previous value, and set the control
        if (shv == pv){
            z_isStepTooSmall = true;
        } 
        //--4. reset the slider value in int.
        z_slider.setHighValue(shv);
    }


    /**
     * Get the lower value of this slider.<p/>
     * @return 
     */
    public Number getLowValue(){
        return z_lowValue;
    }

    /**
     * Do nothing. Leave it for future extention.
     */
    public void setLowValue(){
        //TODO: not sure why need this function. But just give it so other can
        //      extends and find useful applications.
    }

    /**
     * Get the higher value of this slider.<p/>
     * @return 
     */
    public Number getHighValue(){
        return z_highValue;
    }

    /**
     * Do nothing. Leave it for future extention.
     */
    public void setHighValue(){
        //TODO: not sure why need this function. But just give it so other can
        //      extends and find useful applications.
    }

    /**
     * Add ChangeListener
     * @param cl
     */
    @SuppressWarnings("unchecked")
	public void addChangeListener(ChangeListener cl){
        if (!z_listeners.contains(cl)){
            z_listeners.add(cl);
        }
    }

    /**
     * Remove changelistener
     * @param cl
     */
    public void removeChangeListener(ChangeListener cl){
        z_listeners.remove(cl);
    }

    /**
     * Fire change event
     */
    public void fireChangeEvent(){
        if (z_event == null){
            z_event = new ChangeEvent(this);
        }
        for (int i=0;i<z_listeners.size();i++){
            ((ChangeListener) z_listeners.get(i)).stateChanged(z_event);
        }
    }


}
