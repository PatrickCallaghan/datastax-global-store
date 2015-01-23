package com.datastax.demo.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class ByteUtils {

	public static ByteBuffer toByteBuffer(Object obj) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();
		return ByteBuffer.wrap(baos.toByteArray());
	}

	public static Object fromByteBuffer(ByteBuffer bytes) throws Exception {
		if ((bytes == null) || !bytes.hasRemaining()) {
			return null;
		}
		int l = bytes.remaining();
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes.array(), bytes.arrayOffset() + bytes.position(), l);
		ObjectInputStream ois;

		ois = new ObjectInputStream(bais);
		Object obj = ois.readObject();
		bytes.position(bytes.position() + (l - ois.available()));
		ois.close();
		return obj;
	}
}
