package org.hipi.util;

import java.io.IOException;

import org.dcm4che3.data.Tag;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.ElementDictionary;
import org.dcm4che3.data.Fragments;
import org.dcm4che3.data.Sequence;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputHandler;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.util.TagUtils;


/**
 * @author Gunter Zeilinger <gunterze@gmail.com>
 */
public class DcmDump implements DicomInputHandler {

	/** default number of characters per line */
	private static final int DEFAULT_WIDTH = 78;

	private int width;
	private static StringBuilder stringBuilder;

	public DcmDump() {
		width = DEFAULT_WIDTH;
		stringBuilder = new StringBuilder("Start Dicom File\n");
	}

	public StringBuilder getStringBuilder() { return stringBuilder; }

	public final int getWidth() {
		return width;
	}

	public final void setWidth(int width) {
		if (width < 40)
			throw new IllegalArgumentException();
		this.width = width;
	}

	public void parse(DicomInputStream dis) throws IOException {
		dis.setDicomInputHandler(this);
		dis.readDataset(-1, -1);
	}

	@Override
	public void startDataset(DicomInputStream dis) throws IOException {
		promptPreamble(dis.getPreamble());
	}

	@Override
	public void endDataset(DicomInputStream dis) throws IOException {
	}

	@Override
	public void readValue(DicomInputStream dis, Attributes attrs)
			throws IOException {

		StringBuilder line = new StringBuilder(width + 30);
		appendPrefix(dis, line);
		appendHeader(dis, line);
		VR vr = dis.vr();
		int vallen = dis.length();
		boolean undeflen = vallen == -1;
		if (vr == VR.SQ || undeflen) {
			appendKeyword(dis, line);

			stringBuilder.append(line+"\n");

			dis.readValue(dis, attrs);
			if (undeflen) {
				line.setLength(0);
				appendPrefix(dis, line);
				appendHeader(dis, line);
				appendKeyword(dis, line);

				stringBuilder.append(line+"\n");
			}
			return;
		}
		int tag = dis.tag();
		byte[] b = dis.readValue();
		line.append(" [");
		if (vr.prompt(b, dis.bigEndian(), attrs.getSpecificCharacterSet(), width - line.length() - 1, line)) {
			line.append(']');
			appendKeyword(dis, line);
		}
		stringBuilder.append(line+"\n");

		if (tag == Tag.FileMetaInformationGroupLength)
			dis.setFileMetaInformationGroupLength(b);
		else if (tag == Tag.TransferSyntaxUID
				|| tag == Tag.SpecificCharacterSet
				|| TagUtils.isPrivateCreator(tag))
			attrs.setBytes(tag, vr, b);
	}

	@Override
	public void readValue(DicomInputStream dis, Sequence seq)
			throws IOException {
		StringBuilder line = new StringBuilder(width);
		appendPrefix(dis, line);
		appendHeader(dis, line);
		appendKeyword(dis, line);
		appendNumber(seq.size() + 1, line);

		stringBuilder.append(line+"\n");

		boolean undeflen = dis.length() == -1;
		dis.readValue(dis, seq);
		if (undeflen) {
			line.setLength(0);
			appendPrefix(dis, line);
			appendHeader(dis, line);
			appendKeyword(dis, line);

			stringBuilder.append(line+"\n");
		}
	}

	@Override
	public void readValue(DicomInputStream dis, Fragments frags)
			throws IOException {
		StringBuilder line = new StringBuilder(width + 20);
		appendPrefix(dis, line);
		appendHeader(dis, line);
		appendFragment(line, dis, frags.vr());

		stringBuilder.append(line+"\n");
	}

	private void appendPrefix(DicomInputStream dis, StringBuilder line) {
		line.append(dis.getTagPosition()).append(": ");
		int level = dis.level();
		while (level-- > 0)
			line.append('>');

	}

	private void appendHeader(DicomInputStream dis, StringBuilder line) {
		line.append(TagUtils.toString(dis.tag())).append(' ');
		VR vr = dis.vr();
		if (vr != null)
			line.append(vr).append(' ');
		line.append('#').append(dis.length());
	}

	private void appendKeyword(DicomInputStream dis, StringBuilder line) {
		if (line.length() < width) {
			line.append(" ");
			line.append(ElementDictionary.keywordOf(dis.tag(), null));
			if (line.length() > width)
				line.setLength(width);
		}
	}

	private void appendNumber(int number, StringBuilder line) {
		if (line.length() < width) {
			line.append(" #");
			line.append(number);
			if (line.length() > width)
				line.setLength(width);
		}
	}

	private void appendFragment(StringBuilder line, DicomInputStream dis,
			VR vr) throws IOException {
		byte[] b = dis.readValue();
		line.append(" [");
		if (vr.prompt(b, dis.bigEndian(), null,  width - line.length() - 1, line)) {
			line.append(']');
			appendKeyword(dis, line);
		}
	}

	private void promptPreamble(byte[] preamble) {
		if (preamble == null)
			return;

		StringBuilder line = new StringBuilder(width);
		line.append("0: [");
		if (VR.OB.prompt(preamble, false, null, width - 5, line))
			line.append(']');

		stringBuilder.append(line+"\n");
	}
}
