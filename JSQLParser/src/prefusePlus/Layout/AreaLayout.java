/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package prefusePlus.Layout;

import java.awt.geom.Rectangle2D;
import java.util.Iterator;
import prefuse.Constants;
import prefuse.action.layout.Layout;
import prefuse.util.MathLib;
import prefuse.util.ui.ValuedRangeModel;
import prefuse.visual.VisualItem;

/**
 * <p>Layout to assign a rectangle area to a visual item. The visual item MUST have a data
 * column, named as "hight" to store the height value of an area. The width of
 * an area is determined by the two adjoint visual items.</p>
 * <p>
 * This Layout require to work with AreaShapeRenderer when rendering area visual
 * item.
 * </p>
 * width = 2ndVisualItem.x - 1stVisualItem;<p />
 * hight = Max of Display -top+down.inset<p />
 *
 * @author James
 *
 * TODO: Seperate this layout from data column and create a new visual item to
 * represent a rectangle area.<p />
 *
 * NOTE: The SDSS Log Viewer does not use this layout. This layout can use to create
 * other applications.<p />
 */
public class AreaLayout extends Layout {

    //-- scale of this axis
    private int m_scale=Constants.LINEAR_SCALE;
    private ValuedRangeModel m_RM=null;
    private double[] m_dist=new double[2];
    //-- screen coordinate range
    private double m_min;
    private double m_range;

    public AreaLayout(String group, ValuedRangeModel v_rm){
        m_group=group;
        m_RM=v_rm;
    }

    private void setMinMax(){
        Rectangle2D r2d = getLayoutBounds();
        m_min=r2d.getMaxY();
        m_range=r2d.getMinY()-m_min;
    }

    @Override
    public void run(double d) {
        setMinMax();
        m_dist[0]=((Number)m_RM.getLowValue()).doubleValue();
        m_dist[1]=((Number)m_RM.getHighValue()).doubleValue();

        if (m_RM != null){
            double max=((Number)m_RM.getMaxValue()).doubleValue();
            @SuppressWarnings("rawtypes")
			Iterator it=m_vis.items(m_group);
            while (it.hasNext()){
                VisualItem vi=(VisualItem) it.next();

                double f=MathLib.interp(m_scale, max, m_dist);
                double s_max=m_min+f*m_range;
                setY(vi, null, s_max);
                vi.setDouble("hight", -m_range);
            }
        }

    }

}
