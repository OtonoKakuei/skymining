/*
 *
 */

package prefusePlus.controls;

import java.awt.event.MouseEvent;
import prefuse.Display;
import prefuse.controls.ControlAdapter;
import prefuse.visual.VisualItem;

/**
 * A tool tip control for each line in SDSS Log Viewer's SQL Content View. When
 * mouse hovers on a line, this tool tip appears, and dismiss when mouse leave 
 * the line.<p />
 * @author JZhang<p /><p />
 * Dec. 12th, 2010: Revised to show one string in multiple lines.<p />
 */
public class SQLToolTipControl extends ControlAdapter{

    private String[] m_label;
    private StringBuilder sbder;

    /**
     * Construct a SQLToolTipControl with the specified filed, which will supply
     * string text for tool tip to show.<p />
     * @param field - column of string text.<p />
     */
    public SQLToolTipControl(String field){
        this(new String[] {field});
    }

    /**
     * Construct a SQLToolTipControl with the specified filed array, which will
     * supply a set of string texts to show.<p />
     * @param fields - column array<p />
     */
    public SQLToolTipControl(String[] fields){
        m_label = fields;
        sbder = new StringBuilder();
    }

    @Override
    /**
     * Mouse enters the item, showing the tool tips supplied by the specified 
     * filed or field array.<p />
     */
    public void itemEntered(VisualItem vi, MouseEvent e){

        Display d = (Display) e.getSource();

        if (m_label.length ==1 ){
            if (vi.canGetString(m_label[0])){
                sbder.delete(0, sbder.length());
                sbder.append("<HTML>");
                sbder.append("<font color=\"#FFA500\"><b>");
                sbder.append(m_label[0].toUpperCase());
                sbder.append("</b></font>:");
                String token = (String) vi.get(m_label[0]);
                buildTipText(token);
            } // end canGetString
        } else {
            sbder.delete(0, sbder.length());
            sbder.append("<HTML>");
            for (int i=0;i<m_label.length;i++){
                if (vi.canSetString(m_label[i])){
                    sbder.append("<font color=\"#FFA500\"><b>");
                    sbder.append(m_label[i]);
                    sbder.append("</b></font>:");
                    buildTipText((String) vi.get(m_label[i]));
                } //end canGetString
            } // end for loop
        } // end if == 1

        sbder.append("</HTML>");
        d.setToolTipText(sbder.toString());
    }

    @Override
    /**
     * Mouse leaves the visual item, dismissing the tool tip.<p />
     */
    public void itemExited(VisualItem item, MouseEvent e){
        Display d = (Display) e.getSource();
        d.setToolTipText(null);
    }


    /**
     * Method to build tool tip text from an array of fileds.<p/>
     * 
     * @param tokens<p/>
     * 
     * March 4th, 2011: fix a bug, showing "<" and ">" in tooltips with its html
     * codes.<p />
     * < = "&lt"+;<p/>
     * > = "&gt"+;<p/>
     */
    private void buildTipText(String tokens){

        String[] s = tokens.replaceAll("<", "&lt;").replaceAll(">", "&gt;").split("\\s|\\[br\\]");
        int length = 0;

        for (int i=0;i<s.length;i++){

            length += s[i].length();
//System.out.println(length + "\t" + s[i]);
            if (length < 50){
//                sbder.append("<FONT COLOR=\"");
//                sbder.append(Integer.toHexString(colors.get(i)));
//                sbder.append("\">");
                sbder.append(s[i]);
                sbder.append(' ');
//                sbder.append("</FONT> ");
            } else {
//                sbder.append("<FONT COLOR=\"");
//                sbder.append(Integer.toHexString(colors.get(i)));
//                sbder.append("\">");
                sbder.append("<br />");
                sbder.append(s[i]);
                sbder.append(' ');
                //                sbder.append("</FONT> ");
                length = 0;
            } //END if
        } //end for
        sbder.append("<br />");
    }


}
