
package prefusePlus.util;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.AffineTransform;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Rectangle2D;
import java.awt.geom.RectangularShape;
import java.awt.geom.RoundRectangle2D;
import prefuse.render.AbstractShapeRenderer;
import prefuse.util.ColorLib;
import prefuse.util.GraphicsLib;
import prefuse.visual.VisualItem;


/**
 * <p>A graphics lib to draw a set of shapes with different colors
 * </p>
 * Designed to process the 10 colors in SDSS Log Viewer's SQL Content View.<p/>
 * 
 * <p>Extends from prefuse.util.GraphicLib
 * </p>
 * 
 * @author James
 */
public class GraphicsLibPlus extends GraphicsLib {

    public static int[] m_palette=new int[10];
    public static Shape[] m_shapes=new Shape[10];

    /**
     * method to paint different colors to different shapes for ONE visual item.<p/>
     * @param g - graphics<p/>
     * @param item - the visual item<p/>
     * @param shapes - a shape array<p/>
     * @param colors - a color array<p/>
     * @param stroke - the type of color<p/>
     * @param type - rendering type<p/>
     */
    @SuppressWarnings("unused")
	public static void compositedPaint(Graphics2D g, VisualItem item,
                             Shape[] shapes, int[] colors, BasicStroke stroke, int type){
        // if render type is NONE, then there is nothing to do
        if ( type == AbstractShapeRenderer.RENDER_TYPE_NONE )
            return;

        // set up colors
        Color strokeColor = ColorLib.getColor(item.getStrokeColor());

        // paint all shapes in a loop
        int length=shapes.length;

        for (int i=0; i<length; i++){
            Shape tempS=(Shape) shapes[i];
            Color fillColor = ColorLib.getColor(colors[i]);
            boolean sdraw = (type == AbstractShapeRenderer.RENDER_TYPE_DRAW ||
                             type == AbstractShapeRenderer.RENDER_TYPE_DRAW_AND_FILL) &&
                            strokeColor.getAlpha() != 0;
            boolean fdraw = (type == AbstractShapeRenderer.RENDER_TYPE_FILL ||
                             type == AbstractShapeRenderer.RENDER_TYPE_DRAW_AND_FILL) &&
                            fillColor.getAlpha() != 0;
            if ( !(sdraw || fdraw) ) return;

            Stroke origStroke = null;
            if ( sdraw ) {
                origStroke = g.getStroke();
                g.setStroke(stroke);
            }

            int x, y, w, h, aw, ah;
            double xx, yy, ww, hh;

            // Below is the original comments by Jefferey Heer. He is right, we
            // have to render the details even though it should be done by JAVA
            // see if an optimized (non-shape) rendering call is available for us
            // these can speed things up significantly on the windows JRE
            // it is stupid we have to do this, but we do what we must
            // if we are zoomed in, we have no choice but to use
            // full precision rendering methods.
            AffineTransform at = g.getTransform();
            double scale = Math.max(at.getScaleX(), at.getScaleY());
/*            if ( scale >= 1.0 ) {
                if (fdraw) { g.setPaint(fillColor);   g.fill(tempS); }
                if (sdraw) { g.setPaint(strokeColor); g.draw(tempS); }
            }
            else
*/            if ( tempS instanceof RectangularShape )
            {
                RectangularShape r = (RectangularShape)tempS;
                xx = r.getX(); ww = r.getWidth();
                yy = r.getY(); hh = r.getHeight();

                x = (int)xx;
                y = (int)yy;
                w = (int)(ww+xx-x);
                h = (int)(hh+yy-y);

                if ( tempS instanceof Rectangle2D ) {
                    if (fdraw) {
                        g.setPaint(fillColor);
                        g.fillRect(x, y, w, h);
                    }
                    if (sdraw) {
                        g.setPaint(strokeColor);
                        g.drawRect(x, y, w, h);
                    }
                } else if ( tempS instanceof RoundRectangle2D ) {
                    RoundRectangle2D rr = (RoundRectangle2D)tempS;
                    aw = (int)rr.getArcWidth();
                    ah = (int)rr.getArcHeight();
                    if (fdraw) {
                        g.setPaint(fillColor);
                        g.fillRoundRect(x, y, w, h, aw, ah);
                    }
                    if (sdraw) {
                        g.setPaint(strokeColor);
                        g.drawRoundRect(x, y, w, h, aw, ah);
                    }
                } else if ( tempS instanceof Ellipse2D ) {
                    if (fdraw) {
                        g.setPaint(fillColor);
                        g.fillOval(x, y, w, h);
                    }
                    if (sdraw) {
                        g.setPaint(strokeColor);
                        g.drawOval(x, y, w, h);
                    }
                } else {
                    if (fdraw) { g.setPaint(fillColor);   g.fill(tempS); }
                    if (sdraw) { g.setPaint(strokeColor); g.draw(tempS); }
                }
            } else {
                if (fdraw) { g.setPaint(fillColor);   g.fill(tempS); }
                if (sdraw) { g.setPaint(strokeColor); g.draw(tempS); }
            }
            if ( sdraw ) {
                g.setStroke(origStroke);
            }

        }   //end of drawing loop
    }

}   //end of GraphicsLibPlus
