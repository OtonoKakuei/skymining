
package prefusePlus.controls;

import java.awt.event.MouseEvent;
import prefuse.Display;
import prefuse.controls.ControlAdapter;

/**
 * A new tool tip control, used to show legend of SDSS Log Viewer's SQL Content
 * View.<p />
 * @author jzhang
 */
public class SQLLegendToolTipControl extends ControlAdapter {

    /* The HTML codes that show the legend texts.*/
    String legendHTML = "<html>" +
            "<font size=5, color=\"#FFFFFF\"><b>Each line in this view represents a SQL query. </b><font><br>" +
            "<font size=5, color=\"#FFFFFF\"><b>The color bars in each line represent the tokens in a query</b><br><br>" +
            "<font size=5, color=\"#FF0000\"><b>&mdash; Red token represents MS SQL keywords, e.g. \"Select\", \"Join\"...</b></font><br>" +
            "<font size=5, color=\"#FFC0CB\"><b>&mdash; Pink token represents MS SQL functions, e.g. \"top\", \"count\"...</b></font><br>" +
            "<font size=5, color=\"#0000FF\"><b>&mdash; Blue token represents MS SQL operators, e.g. \"+\", \"-\", \"=\"...</b></font><br>" +
            "<font size=5, color=\"#00FF00\"><b>&mdash; Green token represents users' input, e.g. column names...</b></font><br>" +
            "<font size=5, color=\"#FFFF00\"><b>&mdash; Yellow token represents MS SQL special chars, e.g. \"(\", \")\"...</b></font><br>" +
            "<font size=5, color=\"#000000\"><b>&mdash; Black token represents string in quotation, e.g Flags, or Strings...</b></font><br>" +
            "<font size=5, color=\"#FFA500\"><b>&mdash; Orange token represents SDSS user defined functions, e.g. \"fGetNearbyOjbEq\"...</b></font><br>" +
            "<font size=5, color=\"#FFFFFF\"><b>&mdash; White token represents comments in SQL queries</b></font><br>" +
            "<font size=5, color=\"#00FFFF\"><b>&mdash; Cysn token represents SDSS tables, e.g. \"Photoz\", \"Field\"...</b></font><br>" +
            "<font size=5, color=\"#808080\"><b>&mdash; Gray token represents SDSS views, e.g. \"Galaxy\", \"Star\"...</b></font><br>" +
            "</html>";

    /**
     * Construct a new SQLLegendToolTipControl.<p />
     */
    public SQLLegendToolTipControl(){
        super();
    }

    /**
     * Mouse enters into a Display, triggering this tool tip to appear.<p />
     * @param e 
     */
    public void mouseEntered(MouseEvent e){
        Display d = (Display) e.getSource();
        d.setToolTipText(legendHTML);
    }

    /**
     * Mouse leave a Display, dismissing the tool tip.<p />
     * @param e 
     */
    public void mouseExited(MouseEvent e){
        Display d = (Display) e.getSource();
        d.setToolTipText(null);
    }

}
