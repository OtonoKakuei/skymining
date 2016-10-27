package net.sf.jsqlparser.test.select;


public class MemoryTest {

	public static void main(String[] args) throws Exception {
		System.gc();
		System.out.println(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
		System.gc();
		System.out.println(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

	}
}
