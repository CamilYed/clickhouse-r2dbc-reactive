package io.github.camilyed.clickhouse.r2dbc.testkit.abilities;

import io.netty.buffer.ByteBuf;

/** Test DSL: converts a Netty {@link ByteBuf} to a plain byte array for assertions. */
public interface ToByteArrayAbility {

    default byte[] toByteArray(final ByteBuf byteBuf) {
        final byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        return bytes;
    }
}
