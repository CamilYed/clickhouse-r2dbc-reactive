package io.github.camilyed.clickhouse.r2dbc.testkit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Hand-built ClickHouse wire bytes for controlled-server test scenarios.
 *
 * <p>Encodes the same {@code RowBinaryWithNamesAndTypes} shape that {@code
 * com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader} decodes: a
 * {@code VarUInt} column count, the column names as {@code String}s, the column types as {@code
 * String}s, then the row values with no further framing.
 */
public final class ClickHouseWireFixtures {

    private ClickHouseWireFixtures() {}

    /** One column named {@code "1"} of type {@code UInt8}, one row with value {@code 1}. */
    public static byte[] selectOneRowBinaryWithNamesAndTypes() {
        return rowBinaryWithNamesAndTypes(new String[] {"1"}, new String[] {"UInt8"}, new byte[] {0x01});
    }

    private static byte[] rowBinaryWithNamesAndTypes(
            final String[] columnNames, final String[] columnTypes, final byte[] rowBytes) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            writeVarUInt(out, columnNames.length);
            for (final String name : columnNames) {
                writeString(out, name);
            }
            for (final String type : columnTypes) {
                writeString(out, type);
            }
            out.write(rowBytes);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static void writeString(final ByteArrayOutputStream out, final String value) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeVarUInt(out, bytes.length);
        out.write(bytes);
    }

    private static void writeVarUInt(final ByteArrayOutputStream out, final int value) {
        int remaining = value;
        while (true) {
            if ((remaining & ~0x7F) == 0) {
                out.write(remaining);
                return;
            }
            out.write((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
    }
}
