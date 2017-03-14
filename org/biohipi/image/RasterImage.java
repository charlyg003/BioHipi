package org.biohipi.image;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImage.BioHipiImageType;
import org.biohipi.image.BioHipiImageHeader.BioHipiColorSpace;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.util.ByteUtils;
import org.biohipi.image.BioHipiImage;

import java.awt.image.BufferedImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.IllegalArgumentException;

/**
 * A raster image represented as an array of Java floats, which represents a flat array of 
 * uncompressed image pixel data stored in interleaved raster-scan order (e.g., RGBRGBRGB...).
 * A RasterImage extends the abstract base class {@link BioHipiImage} and consists 
 * of a {@link BioHipiImageHeader}.
 *<br>
 *
 * Note that individual pixel values in a RasterImage are understood to be in a linear
 * color space. We suggest using the {@link #setElemNonLinSRGB} method to
 * set pixel values from 8-bit pixel values that are read from an image as these are
 * usually represented in a non-linear gamma-compressed color space.
 *
 * The {@link org.hipi.image.io} package provides classes for reading
 * (decoding) and writing (encoding) RasterImage objects in various
 * compressed and uncompressed image formats such as JPEG and PNG.
 */
public class RasterImage extends BioHipiImage {

	private float[] data;

	/**
	 * Default constructor.
	 * 
	 *  @see BioHipiImage#BioHipiImage()
	 */
	public RasterImage() {
		super();
	}

	/**
	 * Creates a new RasterImage with only header.
	 * 
	 * @param header with meta data information.
	 */
	public RasterImage(BioHipiImageHeader header) {
		this.header = header;
		int size = this.getWidth()*this.getHeight()*this.getNumBands();
		if (size < 0) {
			throw new IllegalArgumentException("Invalid size of pixel array.");
		}
		this.data = size == 0 ? null : new float[size];
	}

	/**
	 * Creates a new RasterImage with data pixels.
	 * 
	 * @param header with meta data information.
	 * @param data contains the pixel values.
	 */
	public RasterImage(BioHipiImageHeader header, float data[]) {
		this(header);
		this.data = data;
	}

	/**
	 * Creates a new RasterImage.
	 * 
	 * @param header with meta data information.
	 * @param javaImage containing serialized image data.
	 */
	public RasterImage(BioHipiImageHeader header, BufferedImage javaImage) {
		this(header);

		int w = javaImage.getWidth();
		int h = javaImage.getHeight();

		// Check that image dimensions in header match those in JPEG
		if (w != this.getWidth() || h != this.getHeight()) {
			System.out.println(String.format("Dimensions read from JPEG: %d x %d", w, h));
			System.out.println(header);
			throw new IllegalArgumentException("Image dimensions in header do not match those in JPEG.");
		}

		for (int j = 0; j < h; j++) {
			for (int i = 0; i < w; i++) {

				// Retrieve 8-bit non-linear sRGB value packed into int
				int pixel = javaImage.getRGB(i, j);

				int red = (pixel >> 16) & 0xff;
				int grn = (pixel >> 8) & 0xff;
				int blu = (pixel) & 0xff;

				// Set value in pixel array using routine designed for sRGB values
				setElemNonLinSRGB((j * w + i) * 3 + 0, red);
				setElemNonLinSRGB((j * w + i) * 3 + 1, grn);
				setElemNonLinSRGB((j * w + i) * 3 + 2, blu);
			}
		}

	}

	private static final int[] gammaCompress = {0,2,4,5,7,8,10,11,13,14,15,17,18,19,20,21,22,22,23,24,25,26,27,27,28,29,29,30,31,31,32,33,33,34,35,35,36,36,37,37,38,38,39,40,40,41,41,42,42,43,43,43,44,44,45,45,46,46,47,47,47,48,48,49,49,50,50,50,51,51,52,52,52,53,53,53,54,54,55,55,55,56,56,56,57,57,57,58,58,58,59,59,60,60,60,60,61,61,61,62,62,62,63,63,63,64,64,64,65,65,65,65,66,66,66,67,67,67,68,68,68,68,69,69,69,70,70,70,70,71,71,71,71,72,72,72,73,73,73,73,74,74,74,74,75,75,75,75,76,76,76,76,77,77,77,77,78,78,78,78,79,79,79,79,80,80,80,80,81,81,81,81,81,82,82,82,82,83,83,83,83,84,84,84,84,84,85,85,85,85,86,86,86,86,86,87,87,87,87,87,88,88,88,88,89,89,89,89,89,90,90,90,90,90,91,91,91,91,91,92,92,92,92,92,93,93,93,93,93,94,94,94,94,94,95,95,95,95,95,96,96,96,96,96,96,97,97,97,97,97,98,98,98,98,98,99,99,99,99,99,99,100,100,100,100,100,101,101,101,101,101,101,102,102,102,102,102,102,103,103,103,103,103,104,104,104,104,104,104,105,105,105,105,105,105,106,106,106,106,106,106,107,107,107,107,107,107,108,108,108,108,108,108,109,109,109,109,109,109,110,110,110,110,110,110,111,111,111,111,111,111,111,112,112,112,112,112,112,113,113,113,113,113,113,114,114,114,114,114,114,114,115,115,115,115,115,115,115,116,116,116,116,116,116,117,117,117,117,117,117,117,118,118,118,118,118,118,118,119,119,119,119,119,119,120,120,120,120,120,120,120,121,121,121,121,121,121,121,122,122,122,122,122,122,122,123,123,123,123,123,123,123,123,124,124,124,124,124,124,124,125,125,125,125,125,125,125,126,126,126,126,126,126,126,127,127,127,127,127,127,127,127,128,128,128,128,128,128,128,129,129,129,129,129,129,129,129,130,130,130,130,130,130,130,130,131,131,131,131,131,131,131,132,132,132,132,132,132,132,132,133,133,133,133,133,133,133,133,134,134,134,134,134,134,134,134,135,135,135,135,135,135,135,135,136,136,136,136,136,136,136,136,137,137,137,137,137,137,137,137,138,138,138,138,138,138,138,138,138,139,139,139,139,139,139,139,139,140,140,140,140,140,140,140,140,141,141,141,141,141,141,141,141,141,142,142,142,142,142,142,142,142,143,143,143,143,143,143,143,143,143,144,144,144,144,144,144,144,144,144,145,145,145,145,145,145,145,145,146,146,146,146,146,146,146,146,146,147,147,147,147,147,147,147,147,147,148,148,148,148,148,148,148,148,148,149,149,149,149,149,149,149,149,149,150,150,150,150,150,150,150,150,150,151,151,151,151,151,151,151,151,151,151,152,152,152,152,152,152,152,152,152,153,153,153,153,153,153,153,153,153,154,154,154,154,154,154,154,154,154,154,155,155,155,155,155,155,155,155,155,155,156,156,156,156,156,156,156,156,156,157,157,157,157,157,157,157,157,157,157,158,158,158,158,158,158,158,158,158,158,159,159,159,159,159,159,159,159,159,159,160,160,160,160,160,160,160,160,160,160,161,161,161,161,161,161,161,161,161,161,162,162,162,162,162,162,162,162,162,162,163,163,163,163,163,163,163,163,163,163,164,164,164,164,164,164,164,164,164,164,165,165,165,165,165,165,165,165,165,165,165,166,166,166,166,166,166,166,166,166,166,167,167,167,167,167,167,167,167,167,167,167,168,168,168,168,168,168,168,168,168,168,169,169,169,169,169,169,169,169,169,169,169,170,170,170,170,170,170,170,170,170,170,170,171,171,171,171,171,171,171,171,171,171,172,172,172,172,172,172,172,172,172,172,172,173,173,173,173,173,173,173,173,173,173,173,174,174,174,174,174,174,174,174,174,174,174,174,175,175,175,175,175,175,175,175,175,175,175,176,176,176,176,176,176,176,176,176,176,176,177,177,177,177,177,177,177,177,177,177,177,178,178,178,178,178,178,178,178,178,178,178,178,179,179,179,179,179,179,179,179,179,179,179,180,180,180,180,180,180,180,180,180,180,180,180,181,181,181,181,181,181,181,181,181,181,181,181,182,182,182,182,182,182,182,182,182,182,182,183,183,183,183,183,183,183,183,183,183,183,183,184,184,184,184,184,184,184,184,184,184,184,184,185,185,185,185,185,185,185,185,185,185,185,185,186,186,186,186,186,186,186,186,186,186,186,186,187,187,187,187,187,187,187,187,187,187,187,187,188,188,188,188,188,188,188,188,188,188,188,188,188,189,189,189,189,189,189,189,189,189,189,189,189,190,190,190,190,190,190,190,190,190,190,190,190,191,191,191,191,191,191,191,191,191,191,191,191,191,192,192,192,192,192,192,192,192,192,192,192,192,192,193,193,193,193,193,193,193,193,193,193,193,193,194,194,194,194,194,194,194,194,194,194,194,194,194,195,195,195,195,195,195,195,195,195,195,195,195,195,196,196,196,196,196,196,196,196,196,196,196,196,196,197,197,197,197,197,197,197,197,197,197,197,197,197,198,198,198,198,198,198,198,198,198,198,198,198,198,199,199,199,199,199,199,199,199,199,199,199,199,199,200,200,200,200,200,200,200,200,200,200,200,200,200,200,201,201,201,201,201,201,201,201,201,201,201,201,201,202,202,202,202,202,202,202,202,202,202,202,202,202,203,203,203,203,203,203,203,203,203,203,203,203,203,203,204,204,204,204,204,204,204,204,204,204,204,204,204,204,205,205,205,205,205,205,205,205,205,205,205,205,205,206,206,206,206,206,206,206,206,206,206,206,206,206,206,207,207,207,207,207,207,207,207,207,207,207,207,207,207,208,208,208,208,208,208,208,208,208,208,208,208,208,208,209,209,209,209,209,209,209,209,209,209,209,209,209,209,210,210,210,210,210,210,210,210,210,210,210,210,210,210,211,211,211,211,211,211,211,211,211,211,211,211,211,211,211,212,212,212,212,212,212,212,212,212,212,212,212,212,212,213,213,213,213,213,213,213,213,213,213,213,213,213,213,214,214,214,214,214,214,214,214,214,214,214,214,214,214,214,215,215,215,215,215,215,215,215,215,215,215,215,215,215,215,216,216,216,216,216,216,216,216,216,216,216,216,216,216,217,217,217,217,217,217,217,217,217,217,217,217,217,217,217,218,218,218,218,218,218,218,218,218,218,218,218,218,218,218,219,219,219,219,219,219,219,219,219,219,219,219,219,219,219,220,220,220,220,220,220,220,220,220,220,220,220,220,220,220,221,221,221,221,221,221,221,221,221,221,221,221,221,221,221,222,222,222,222,222,222,222,222,222,222,222,222,222,222,222,222,223,223,223,223,223,223,223,223,223,223,223,223,223,223,223,224,224,224,224,224,224,224,224,224,224,224,224,224,224,224,225,225,225,225,225,225,225,225,225,225,225,225,225,225,225,225,226,226,226,226,226,226,226,226,226,226,226,226,226,226,226,227,227,227,227,227,227,227,227,227,227,227,227,227,227,227,227,228,228,228,228,228,228,228,228,228,228,228,228,228,228,228,228,229,229,229,229,229,229,229,229,229,229,229,229,229,229,229,229,230,230,230,230,230,230,230,230,230,230,230,230,230,230,230,230,231,231,231,231,231,231,231,231,231,231,231,231,231,231,231,231,232,232,232,232,232,232,232,232,232,232,232,232,232,232,232,232,233,233,233,233,233,233,233,233,233,233,233,233,233,233,233,233,234,234,234,234,234,234,234,234,234,234,234,234,234,234,234,234,234,235,235,235,235,235,235,235,235,235,235,235,235,235,235,235,235,236,236,236,236,236,236,236,236,236,236,236,236,236,236,236,236,236,237,237,237,237,237,237,237,237,237,237,237,237,237,237,237,237,238,238,238,238,238,238,238,238,238,238,238,238,238,238,238,238,238,239,239,239,239,239,239,239,239,239,239,239,239,239,239,239,239,239,240,240,240,240,240,240,240,240,240,240,240,240,240,240,240,240,240,241,241,241,241,241,241,241,241,241,241,241,241,241,241,241,241,241,242,242,242,242,242,242,242,242,242,242,242,242,242,242,242,242,242,243,243,243,243,243,243,243,243,243,243,243,243,243,243,243,243,243,244,244,244,244,244,244,244,244,244,244,244,244,244,244,244,244,244,245,245,245,245,245,245,245,245,245,245,245,245,245,245,245,245,245,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,246,247,247,247,247,247,247,247,247,247,247,247,247,247,247,247,247,247,248,248,248,248,248,248,248,248,248,248,248,248,248,248,248,248,248,248,249,249,249,249,249,249,249,249,249,249,249,249,249,249,249,249,249,249,250,250,250,250,250,250,250,250,250,250,250,250,250,250,250,250,250,250,251,251,251,251,251,251,251,251,251,251,251,251,251,251,251,251,251,251,252,252,252,252,252,252,252,252,252,252,252,252,252,252,252,252,252,252,253,253,253,253,253,253,253,253,253,253,253,253,253,253,253,253,253,253,254,254,254,254,254,254,254,254,254,254,254,254,254,254,254,254,254,254,255};

	public int getElemNonLinSRGB(int i) {
		double linear = (double)data[i];
		int lutIdx = Math.max(0,Math.min(2048,(int)(linear*(double)(2048-1)-0.5)));
		return gammaCompress[lutIdx];
	}

	// LUT for nonlinear 8-bit sRGB => linear floating point RGB
	private static final float[] gammaExpand = {0.0f,3.035269910469651E-4f,6.070539820939302E-4f,9.105809731408954E-4f,0.0012141079641878605f,0.0015176349552348256f,0.0018211619462817907f,0.002124688820913434f,0.002428215928375721f,0.0027317428030073643f,0.0030352699104696512f,0.0033465358428657055f,0.0036765073891729116f,0.004024717025458813f,0.0043914420530200005f,0.004776953253895044f,0.005181516520678997f,0.005605391692370176f,0.006048833020031452f,0.006512090563774109f,0.006995410192757845f,0.007499032188206911f,0.00802319310605526f,0.008568125776946545f,0.009134058840572834f,0.009721217676997185f,0.01032982300966978f,0.010960093699395657f,0.011612244881689548f,0.012286487966775894f,0.012983032502233982f,0.013702083379030228f,0.014443843625485897f,0.015208514407277107f,0.01599629409611225f,0.016807375475764275f,0.017641954123973846f,0.01850022003054619f,0.019382361322641373f,0.020288562402129173f,0.021219009533524513f,0.022173885256052017f,0.023153366521000862f,0.024157632142305374f,0.02518685907125473f,0.026241222396492958f,0.027320891618728638f,0.028426039963960648f,0.02955683507025242f,0.03071344457566738f,0.03189603239297867f,0.03310476616024971f,0.03433980792760849f,0.03560131415724754f,0.03688944876194f,0.0382043719291687f,0.039546236395835876f,0.040915198624134064f,0.0423114113509655f,0.04373503103852272f,0.045186202973127365f,0.04666508734226227f,0.04817182570695877f,0.04970656707882881f,0.05126945674419403f,0.052860647439956665f,0.05448027700185776f,0.05612849071621895f,0.05780543014407158f,0.05951123684644699f,0.061246052384376526f,0.06301001459360123f,0.06480326503515244f,0.0666259378194809f,0.06847816705703735f,0.07036009430885315f,0.07227185368537903f,0.07421357184648514f,0.07618538290262222f,0.07818742096424103f,0.0802198201417923f,0.08228270709514618f,0.08437620848417282f,0.08650045841932297f,0.08865558356046677f,0.09084171056747437f,0.09305896610021591f,0.09530746936798096f,0.09758734703063965f,0.09989872574806213f,0.10224173218011856f,0.10461648553609848f,0.10702310502529144f,0.109461709856987f,0.1119324266910553f,0.11443537473678589f,0.11697066575288773f,0.11953842639923096f,0.12213877588510513f,0.12477181851863861f,0.12743768095970154f,0.13013647496700287f,0.13286831974983215f,0.13563333451747894f,0.1384316086769104f,0.14126329123973846f,0.1441284716129303f,0.14702726900577545f,0.14995978772640228f,0.15292614698410034f,0.15592646598815918f,0.15896083414554596f,0.16202937066555023f,0.16513219475746155f,0.16826939582824707f,0.17144110798835754f,0.17464740574359894f,0.177888423204422f,0.18116424977779388f,0.18447498977184296f,0.18782077729701996f,0.19120168685913086f,0.1946178376674652f,0.19806931912899017f,0.2015562504529953f,0.20507873594760895f,0.20863686501979828f,0.21223075687885284f,0.2158605009317398f,0.21952620148658752f,0.22322796285152435f,0.22696587443351746f,0.23074005544185638f,0.2345505803823471f,0.23839756846427917f,0.24228112399578094f,0.2462013214826584f,0.25015828013420105f,0.2541520893573761f,0.2581828534603119f,0.2622506618499756f,0.26635560393333435f,0.27049779891967773f,0.2746773064136505f,0.2788942754268646f,0.28314873576164246f,0.28744083642959595f,0.2917706370353699f,0.2961382567882538f,0.30054378509521484f,0.3049873113632202f,0.30946892499923706f,0.31398871541023254f,0.31854677200317383f,0.32314321398735046f,0.3277781009674072f,0.3324515223503113f,0.33716362714767456f,0.34191441535949707f,0.34670406579971313f,0.35153260827064514f,0.35640013217926025f,0.3613067865371704f,0.366252601146698f,0.37123769521713257f,0.3762621283531189f,0.38132601976394653f,0.38642942905426025f,0.3915724754333496f,0.3967552185058594f,0.4019777774810791f,0.40724021196365356f,0.4125426113605499f,0.41788506507873535f,0.423267662525177f,0.42869049310684204f,0.43415364623069763f,0.43965718150138855f,0.44520118832588196f,0.4507857859134674f,0.4564110338687897f,0.46207699179649353f,0.4677838087081909f,0.47353148460388184f,0.4793201684951782f,0.48514994978904724f,0.4910208582878113f,0.4969329833984375f,0.5028864741325378f,0.5088813304901123f,0.5149176716804504f,0.520995557308197f,0.5271151065826416f,0.533276379108429f,0.5394794940948486f,0.5457244515419006f,0.5520114302635193f,0.5583403706550598f,0.5647115111351013f,0.5711248517036438f,0.577580451965332f,0.5840784311294556f,0.5906188488006592f,0.5972017645835876f,0.6038273572921753f,0.6104955673217773f,0.6172065734863281f,0.6239603757858276f,0.6307571530342102f,0.637596845626831f,0.6444796919822693f,0.6514056324958801f,0.6583748459815979f,0.6653872728347778f,0.672443151473999f,0.6795424818992615f,0.68668532371521f,0.6938717365264893f,0.7011018991470337f,0.7083757519721985f,0.715693473815918f,0.7230551242828369f,0.7304607629776001f,0.7379103899002075f,0.7454041838645935f,0.7529422044754028f,0.7605245113372803f,0.7681511640548706f,0.7758222222328186f,0.7835378050804138f,0.7912979125976562f,0.7991027235984802f,0.8069522380828857f,0.8148465752601624f,0.8227857351303101f,0.8307698965072632f,0.838798999786377f,0.8468732237815857f,0.8549926280975342f,0.8631572127342224f,0.8713670969009399f,0.8796223998069763f,0.8879231214523315f,0.8962693810462952f,0.9046611785888672f,0.9130986332893372f,0.9215818643569946f,0.9301108717918396f,0.9386857151985168f,0.9473065137863159f,0.9559733271598816f,0.9646862745285034f,0.9734452962875366f,0.9822505712509155f,0.9911020994186401f,1.0f};

	public void setElemNonLinSRGB(int i, int val) {
		data[i] = gammaExpand[(val < 0 ? 0 : (val > 255 ? 255 : val))];
	}

	public int getIntElem(int i) {
		return (int)(Math.max(0,Math.min(255,(int)(this.data[i]*255.0f))));
	}

	/**
	 * Get color space of image.
	 *
	 * @return color space of image
	 */
	public BioHipiColorSpace getColorSpace() {
		return BioHipiColorSpace.valueOf(header.getMetaData(BioHipiKeyMetaData.COLOR_SPACE));
	}

	/**
	 * Get width of image.
	 *
	 * @return width of image
	 */
	public int getWidth() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.WIDTH));
	}

	/**
	 * Get height of image.
	 *
	 * @return height of image
	 */
	public int getHeight() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.HEIGHT));
	}

	/**
	 * Get number of bands (also called "channels") in image.
	 *
	 * @return number of color bands in image
	 */
	public int getNumBands() {
		return Integer.parseInt(header.getMetaData(BioHipiKeyMetaData.BANDS));
	}
	/**
	 *  Provides direct access to underlying float array of pixel data.
	 * @return data floats array
	 */
	public float[] getData() {
		return this.data;
	}

	/**
	 * Compares two RasterImage objects for equality allowing for some
	 * amount of differences in pixel values.
	 *
	 * @return True if the two images have equal dimensions, color
	 * spaces, and are found to deviate by less than a specified maximum
	 * difference, false otherwise.
	 */
	public boolean equalsWithTolerance(RasterImage thatImage, float maxDifference) {
		if (thatImage == null) {
			return false;
		}
		// Verify dimensions in headers are equal
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		if (this.getColorSpace() != thatImage.getColorSpace() ||
				thatImage.getWidth() != w || thatImage.getHeight() != h || 
				thatImage.getNumBands() != b) {
			return false;
		}

		// Check that pixel data is equal.
		for (int i=0; i<w*h*b; i++) {

			double diff = Math.abs(this.data[i]-thatImage.getData()[i]);

			if (diff > maxDifference) {
				return false;
			}
		}

		// Passed, declare equality
		return true;
	}

	/**
	 * Compares two RasterImage objects for equality.
	 *
	 * @return True if the two images are found to deviate by an amount
	 * that is not representable in the underlying pixel type, false
	 * otherwise.
	 */
	@Override
	public boolean equals(Object that) {
		// Check for pointer equivalence
		if (this == that)
			return true;

		// Verify object types are equal
		if (!(that instanceof RasterImage))
			return false;

		return equalsWithTolerance((RasterImage)that, 0.0f);
	}

	/**
	 * Crops a raster image to a (width x height) rectangular region
	 * with top-left corner at (x,y) pixel location.
	 *
	 * @param x horizontal position of upper left corner of crop rectangle
	 * @param y vertical position of upper left corner of crop rectangle
	 * @param width width of crop rectangle
	 * @param height height of crop rectangle
	 * 
	 * @return {@link RasterImage} cropped
	 */
	public RasterImage crop(int x, int y, int width, int height) {
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();

		// Verify crop dimensions
		if (x < 0 || width <= 0 || x+width > w || y < 0 || height <= 0 || y+height > h) {
			throw new IllegalArgumentException("Invalid crop region.");
		}

		float data[] = new float[width*height*this.getNumBands()];
		// Assemble cropped output
		for (int j=y; j<y+height; j++) {
			for (int i=x; i<x+width; i++) {
				for (int c=0; c<b; c++) {
					data[(j-y)*width+(i-x)] = this.data[(j*w+i)*b+c];
				}
			}
		}

		BioHipiImageHeader header = new BioHipiImageHeader(this.getStorageFormat());
		header.addMetaData(BioHipiKeyMetaData.SOURCE, new String("(Cut) ").concat(this.header.getMetaData(BioHipiKeyMetaData.SOURCE)));
		header.addMetaData("Cut X-Axis", String.format("from %d to %d", x, x+width-1));
		header.addMetaData("Cut Y-Axis", String.format("from %d to %d", y, y+height-1));
		header.addMetaData(BioHipiKeyMetaData.COLOR_SPACE, this.getColorSpace().toString());
		header.addMetaData(BioHipiKeyMetaData.WIDTH, String.valueOf(width));
		header.addMetaData(BioHipiKeyMetaData.HEIGHT, String.valueOf(height));
		header.addMetaData(BioHipiKeyMetaData.BANDS, String.valueOf(this.getNumBands()));


		return new RasterImage(header, data);
	}

	/**
	 * Convert image to another color space.
	 *
	 * @param colorSpace target color space
	 * @param output output {@link RasterImage} target (must be initialized)
	 * 
	 */
	public void convertToColorSpace(BioHipiColorSpace colorSpace, RasterImage output)
			throws IllegalArgumentException {
		if (getColorSpace() == colorSpace) {
			throw new IllegalArgumentException("Cannot convert color space to itself.");
		}
		if (getColorSpace() == BioHipiColorSpace.RGB && output.getColorSpace() == BioHipiColorSpace.LUM) {

			int w = this.getWidth();
			int h = this.getHeight();
			int b = this.getNumBands();

			assert b == 3;

			// Verify color conversion output target
			if (w != output.getWidth() || h != output.getHeight() || 1 != output.getNumBands()) {
				throw new IllegalArgumentException("Invalid dimensions in color convert output target.");
			}

			// Perform color conversion
			for (int j=0; j<h; j++) {
				for (int i=0; i<w; i++) {
					float red = data[(j*w+i)*3+0];
					float grn = data[(j*w+i)*3+1];
					float blu = data[(j*w+i)*3+2];
					float lum = red * 0.30f + grn * 0.59f + blu * 0.11f;
					this.data[j*w+i] = lum;
				}
			}

		} else {
			throw new IllegalArgumentException("Not implemented.");
		}
	}

	/**
	 * Performs in-place addition with another {@link FloatImage}.
	 * 
	 * @param thatImage target image to add to current image
	 *
	 * @throws IllegalArgumentException if the image dimensions do not match
	 */
	public void add(RasterImage thatImage) throws IllegalArgumentException {
		// Verify input
		checkCompatibleInputImage(thatImage);

		// Perform in-place addition
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		for (int i=0; i<w*h*b; i++) {
			this.data[i] += thatImage.getData()[i];
		}
	}

	/**
	 * Performs in-place addition of a scalar to each band of every pixel.
	 * 
	 * @param number scalar value to add to each band of each pixel
	 */
	public void add(float number) {
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		for (int i=0; i<w*h*b; i++) {
			this.data[i] += number;
		}
	}

	/**
	 * Performs in-place element wise multiplication of {@link RasterImage} and the current image.
	 *
	 * @param thatImage target image to use for multiplication
	 */
	public void multiply(RasterImage thatImage) throws IllegalArgumentException {

		// Verify input
		checkCompatibleInputImage(thatImage);

		// Perform in-place elementwise multiply
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		for (int i=0; i<w*h*b; i++) {
			this.data[i] *= thatImage.getData()[i];
		}
	}

	/**
	 * Performs in-place multiplication with scalar.
	 *
	 * @param value Scalar to multiply with each band of each pixel.
	 */
	public void scale(float value) {
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		for (int i=0; i<w*h*b; i++) {
			this.data[i] *= value;
		}
	}

	/**
	 * Helper routine that verifies two images have compatible
	 * dimensions for common operations (addition, elementwise
	 * multiplication, etc.)
	 *
	 * @param image RasterImage to check
	 * 
	 * @throws IllegalArgumentException if the image do not have
	 * compatible dimensions. Otherwise has no effect.
	 */
	protected void checkCompatibleInputImage(RasterImage image) throws IllegalArgumentException {
		if (image.getColorSpace() != this.getColorSpace() || image.getWidth() != this.getWidth() || 
				image.getHeight() != this.getHeight() || image.getNumBands() != this.getNumBands()) {
			throw new IllegalArgumentException("Color space and/or image dimensions do not match.");
		}
	}
	
	/**
	 * Get image type identifier.
	 * 
	 * @return HipiImageType.RASTER
	 * @see BioHipiImageType
	 */
	public BioHipiImageType getType() {
		return BioHipiImageType.RASTER;
	}

	/**
	 * Produces a string representation of the image that concatenates
	 * image dimensions with RGB values of up to first 10 pixels in
	 * raster-scan order.
	 *
	 * @see java.lang.Object#toString
	 */
	@Override
	public String toString() {
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		StringBuilder result = new StringBuilder();
		result.append(String.format("RsterImage: %d x %d x %d [", w, h, b));
		int n = Math.min(10,w*h);
		for (int i=0; i<n; i++) {
			result.append("(");

			for (int c=0; c<b; c++) {
				result.append(String.format("%.2f",this.data[i*b+c]));
				if (c<(b-1))
					result.append(" ");
				else
					result.append(")");
			}

			if (i<(n-1)) {
				result.append(" ");
			}
		}
		result.append("]");
		return result.toString();
	}

	/**
	 * Writes raster image in a simple uncompressed binary format.
	 * @see org.apache.hadoop.io.Writable#write
	 */
	@Override
	public void write(DataOutput output) throws IOException {
		header.write(output);
		output.write(ByteUtils.floatArrayToByteArray(data));
	}

	/**
	 * Reads a raster image stored in a simple uncompressed binary
	 * format.
	 * @see org.apache.hadoop.io.Writable#readFields
	 */
	@Override
	public void readFields(DataInput input) throws IOException {
		// Create and read header
		header = new BioHipiImageHeader(input);
		int w = this.getWidth();
		int h = this.getHeight();
		int b = this.getNumBands();
		int numBytes = w * h * b * 4;
		// Read pixel data
		byte[] pixelBytes = new byte[numBytes];
		input.readFully(pixelBytes);
		if (pixelBytes == null || pixelBytes.length == 0) {
			data = null;
		} else {
			data = ByteUtils.byteArrayToFloatArray(pixelBytes);
		}
	}

}
