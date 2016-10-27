
package prefusePlus.util;

import prefuse.util.ColorLib;

/**
 * Extends from prefuse.util.ColorLib to create more customized color pallete.<p/>
 * @author James<p/>
 * Nov. 4th, 2010:  Class created for more customized color pallete<p/>
 */
public class ColorLibPlus extends ColorLib {

    /** Size of the color space */
    private static final int RANGE = 12;

    /** 12 basic color for color coding, adopted from Colin Ware's book*/
    public static final int RED = ColorLib.rgb(255, 0, 0);
    public static final int GREEN = ColorLib.rgb(0, 255, 0);
    public static final int YELLOW = ColorLib.rgb(255, 255, 0);
    public static final int BLUE = ColorLib.rgb(0, 0, 255);
    public static final int BLACK = ColorLib.rgb(0, 0, 0);
    public static final int WHITE = ColorLib.rgb(255, 255, 255);
    
    public static final int PINK = ColorLib.rgb(255, 192, 203);
    public static final int CYAN = ColorLib.rgb(0, 255, 255);
    public static final int GRAY = ColorLib.rgb(128, 128, 128);
    public static final int ORANGE = ColorLib.rgb(255, 160, 0);
    public static final int BROWN = ColorLib.rgb(165, 42, 42);
    public static final int PURPLE = ColorLib.rgb(128, 0 ,128);

    /** The color pallete for the 12 basic colors */
    private static final int[] BASIC_COLORS = new int[] {RED, PINK, BLUE, YELLOW, BLACK, WHITE,
                                                         GREEN, CYAN, GRAY, ORANGE, BROWN, PURPLE};

    /**
     * Return a color pallete for with all the 12 basic colors
     * @return the color pallete
     */
    public static int[] getBasicColorPallete (){
        return getBasicColorPallete(12);
    }

    /**
     * Return a color pallete start with partial of the 12 basic color
     * @param range the final color range in the pallete. In default start from 0
     * @return the color pallete
     */
    public static int[] getBasicColorPallete(int range){
        if (range > RANGE) {
            range = RANGE;
        }
        int[] cm = new int[range];
        for (int i=0;i<range;i++){
            cm[i] = BASIC_COLORS[i];
        }

        return cm;
    }

}
