package org.hipi.util;

import org.hipi.image.ByteImage;
import org.hipi.image.FloatImage;

public class RGBArray<T> {

	@SuppressWarnings("hiding")
	public class RGBValue<T> {

		private T red;
		private T green;
		private T blue;

		public RGBValue(T red, T green, T blue) {
			this.red = red;
			this.green = green;
			this.blue = blue;
		}

		public T getRed() 	{ return this.red; }
		public T getGreen() { return this.green; }
		public T getBlue() 	{ return this.blue; }

		public String toString() { return String.format("R: %s | G: %s | B: %s", red, green, blue); }

	}

	private int width;
	private int height;

	private T[] data;
	private RGBValue<T>[][] dataRGB;

	@SuppressWarnings("unchecked")
	public RGBArray(int width, int height, T[] data) {
		this.width = width;
		this.height = height;
		this.data = data;
		this.dataRGB = new RGBValue[height][width];
		setDataRGB();
	}

	public RGBValue<T> 		getRGBValue(int x, int y) { return dataRGB[x][y]; }
	public RGBValue<T>[][] 	getDataRGB() { return dataRGB; }

	public int getWidth()  { return width; }
	public int getHeight() { return height; }

	public void setDataRGB() {
		int row = 0, column = 0;

		for (int i = 0; i < data.length; i+=3) {
			dataRGB[row][column] = new RGBValue<T>(data[i], data[i+1], data[i+2]);
			if (column < (width-1))
				column++;
			else {
				row++;
				column = 0;
			}
		}

	}

	/**
	 * get a {@link RGBArray} with float value.
	 * A single value of RGB Array is a RGB Value @see {@link RGBValue}
	 * @param type jpeg or png
	 * @return {@link RGBArray} with float value.
	 */
	public static RGBArray<Float> getRGBFloatArray(FloatImage image) {

		Float[] floatData = new Float[image.getData().length];
		for (int i = 0; i < image.getData().length; i++)
			floatData[i] = image.getData()[i];


		System.out.println("image width "+image.getWidth());
		System.out.println("image height "+image.getHeight());

		return new RGBArray<Float>(image.getWidth(), image.getHeight(),  floatData );
	}

	/**
	 * get a {@link RGBArray} with byte value.
	 * A single value of RGB Array is a RGB Value @see {@link RGBValue}
	 * @param type jpeg or png
	 * @return {@link RGBArray} with byte value.
	 */
	public static RGBArray<Byte> getRGBByteArray(ByteImage image) {

		Byte[] byteData = new Byte[image.getData().length];
		for (int i = 0; i < image.getData().length; i++)
			byteData[i] = image.getData()[i];

		return new RGBArray<Byte>(image.getWidth(), image.getHeight(),  byteData );
	}

}
