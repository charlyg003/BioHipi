/* ***** BEGIN LICENSE BLOCK *****
 * Version: MPL 1.1/GPL 2.0/LGPL 2.1
 *
 * The contents of this file are subject to the Mozilla Public License Version
 * 1.1 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 * for the specific language governing rights and limitations under the
 * License.
 *
 * The Original Code is part of dcm4che, an implementation of DICOM(TM) in
 * Java(TM), hosted at https://github.com/gunterze/dcm4che.
 *
 * The Initial Developer of the Original Code is
 * Agfa Healthcare.
 * Portions created by the Initial Developer are Copyright (C) 2011
 * the Initial Developer. All Rights Reserved.
 *
 * Contributor(s):
 * See @authors listed below
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the MPL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the MPL, the GPL or the LGPL.
 *
 * ***** END LICENSE BLOCK ***** */

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
