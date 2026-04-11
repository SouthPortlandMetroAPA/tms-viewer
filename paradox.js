// ════════════════════════════════════════════════════════════════════════
// paradox.js — Minimal Paradox .DB reader for APA TMS exports
// ════════════════════════════════════════════════════════════════════════
//
// PURPOSE
//   Decodes Borland/Corel Paradox v4+ .DB table files entirely in the
//   browser. Specifically targeted at APA TMS's Tscore8.DB and Tscore9.DB
//   which embed their field names in the header.
//
// SUPPORTED FIELD TYPES
//   0x01 Alpha    — fixed-width ASCII, NUL-padded
//   0x02 Date     — 4-byte days-since-epoch, MSB-biased
//   0x03 Short    — 2-byte signed int, big-endian, MSB-biased
//   0x04 Long     — 4-byte signed int, big-endian, MSB-biased
//   0x06 Number   — 8-byte IEEE 754 double, big-endian, flipped
//   0x09 Logical  — single byte (0x80/0x81)
//   0x0C Memo     — ignored (returns null; stored in .MB sibling)
//
// USAGE
//   const table = parseParadoxDb(arrayBuffer);
//   // → { fieldNames, fieldTypes, fieldSizes, records, recordSize, headerSize, numFields }
//
// CONSTRAINTS
//   Designed for TMS Tscore8/Tscore9 schemas (v4+ embedded field names).
//   Older Paradox v3 tables without embedded field names will parse but
//   field names fall back to field1...fieldN.
// ════════════════════════════════════════════════════════════════════════

(function (global) {
  'use strict';

  // ─── Header layout (Paradox v4+) ──────────────────────────────────
  // offset  size  field
  // 0x00    2     recordSize
  // 0x02    2     headerSize
  // 0x04    1     fileType
  // 0x05    1     maxTableSize (block size = maxTableSize * 1024)
  // 0x06    4     numRecords
  // 0x0A    2     nextBlock
  // 0x0C    2     fileBlocks
  // 0x0E    2     firstBlock
  // 0x10    2     lastBlock
  // 0x21    2     numFields
  // 0x35    1     fileVersionID
  // 0x78    …     field info (type, size) × numFields
  // then    4     tableNamePtr
  // then    4*N   field number array
  // then    79    table name (NUL-padded)
  // then    N*    field names (NUL-terminated ASCII)
  //
  // NOTE: observed layout from APA TMS Tscore8/9 exports puts the
  // table name DIRECTLY after the tableNamePtr and BEFORE the field
  // number array. The parser accepts that order.

  function parseParadoxDb(arrayBuffer) {
    var dv = new DataView(arrayBuffer);
    var bytes = new Uint8Array(arrayBuffer);

    var recordSize   = dv.getUint16(0, true);
    var headerSize   = dv.getUint16(2, true);
    var fileType     = dv.getUint8(4);
    var maxTableSize = dv.getUint8(5);
    var numFields    = dv.getUint16(0x21, true);

    // ── Field info (type, size pairs) ────────────────────────────────
    var fieldStart = 0x78;
    var fieldTypes = new Array(numFields);
    var fieldSizes = new Array(numFields);
    for (var i = 0; i < numFields; i++) {
      fieldTypes[i] = bytes[fieldStart + i * 2];
      fieldSizes[i] = bytes[fieldStart + i * 2 + 1];
    }

    // ── Table name + field number array + field names ──────────────
    // After field info: 4-byte tableNamePtr, then table name (79 bytes),
    // then field number array (4 bytes × numFields), then field names.
    var pos = fieldStart + numFields * 2 + 4; // skip tableNamePtr
    var tableName = readNulString(bytes, pos, 79);
    pos += 79 + 4 * numFields;

    var fieldNames = new Array(numFields);
    for (var j = 0; j < numFields; j++) {
      var end = pos;
      while (end < bytes.length && bytes[end] !== 0) end++;
      if (end > pos) {
        fieldNames[j] = asciiDecode(bytes, pos, end);
      } else {
        fieldNames[j] = 'field' + (j + 1);
      }
      pos = end + 1;
    }

    // ── Data blocks start at offset headerSize ──────────────────────
    var blockSize = maxTableSize * 1024;
    var records = [];
    var blockPos = headerSize;

    while (blockPos + 6 <= bytes.length) {
      // Block header (6 bytes little-endian)
      var addDataSize = dv.getInt16(blockPos + 4, true);
      if (addDataSize >= 0) {
        // numRecs = (addDataSize / recordSize) + 1
        var numRecs = Math.floor(addDataSize / recordSize) + 1;
        var recPos = blockPos + 6;
        for (var r = 0; r < numRecs; r++) {
          if (recPos + recordSize > bytes.length) break;
          var rec = {};
          var fp = recPos;
          for (var f = 0; f < numFields; f++) {
            rec[fieldNames[f]] = parseField(bytes, dv, fp, fieldTypes[f], fieldSizes[f]);
            fp += fieldSizes[f];
          }
          records.push(rec);
          recPos += recordSize;
        }
      }
      blockPos += blockSize;
    }

    return {
      tableName: tableName,
      fieldNames: fieldNames,
      fieldTypes: fieldTypes,
      fieldSizes: fieldSizes,
      records: records,
      recordSize: recordSize,
      headerSize: headerSize,
      numFields: numFields,
      fileType: fileType,
      blockSize: blockSize
    };
  }

  // ─── Field decoders ───────────────────────────────────────────────
  function parseField(bytes, dv, offset, type, size) {
    switch (type) {
      case 0x01: return parseAlpha(bytes, offset, size);
      case 0x02: return parseDate(bytes, offset);
      case 0x03: return parseShort(bytes, offset);
      case 0x04: return parseLong(bytes, offset);
      case 0x06: return parseNumber(bytes, offset);
      case 0x09: return parseLogical(bytes, offset);
      case 0x0C: return null; // memo pointer — ignored
      default:   return null;
    }
  }

  // Alpha: fixed-width ASCII, NUL-padded. Empty if first byte is 0.
  // (APA TMS exports use plain ASCII, not the XOR-encoded DOS format.)
  function parseAlpha(bytes, offset, size) {
    if (bytes[offset] === 0) return null;
    var end = offset;
    var max = offset + size;
    while (end < max && bytes[end] !== 0) end++;
    var s = asciiDecode(bytes, offset, end);
    // Trim trailing whitespace
    return s.replace(/\s+$/, '') || null;
  }

  // Short int: 2 bytes big-endian. Null if both bytes 0x00.
  // Paradox stores shorts with bit 15 XOR'd so they sort correctly
  // as unsigned. To decode: read as BE uint16, subtract 0x8000.
  function parseShort(bytes, offset) {
    var hi = bytes[offset];
    var lo = bytes[offset + 1];
    if (hi === 0 && lo === 0) return null;
    var raw = (hi << 8) | lo;
    return raw - 0x8000;
  }

  // Long int: 4 bytes big-endian. Null if all bytes 0x00.
  function parseLong(bytes, offset) {
    var b0 = bytes[offset];
    var b1 = bytes[offset + 1];
    var b2 = bytes[offset + 2];
    var b3 = bytes[offset + 3];
    if (b0 === 0 && b1 === 0 && b2 === 0 && b3 === 0) return null;
    // Interpret as unsigned BE, subtract 2^31 → signed int32
    var raw = (b0 * 0x1000000) + ((b1 << 16) | (b2 << 8) | b3);
    return raw - 0x80000000;
  }

  // Date: 4 bytes, days since Jan 1, 0001 (Paradox epoch).
  // Stored biased like Long int. Return JS Date or null.
  function parseDate(bytes, offset) {
    var days = parseLong(bytes, offset);
    if (days == null) return null;
    // Paradox date 0 = 12/28/-0001. Days are counted from there.
    // Easiest: Paradox epoch = 1 = Jan 1, 0001 AD. Convert to JS.
    // JS Date: milliseconds since Jan 1, 1970.
    // Days from Jan 1 0001 to Jan 1 1970 = 719162.
    var jsDays = days - 719162;
    return new Date(jsDays * 86400000);
  }

  // Number: 8-byte IEEE 754 double, big-endian, with bit 63 rules:
  //   - If high bit of byte 0 is set → positive value; clear the high bit
  //   - Else (high bit clear) → negative value; invert ALL bits
  // Null when all 8 bytes are 0x00.
  function parseNumber(bytes, offset) {
    var allZero = true;
    for (var i = 0; i < 8; i++) if (bytes[offset + i] !== 0) { allZero = false; break; }
    if (allZero) return null;

    var buf = new ArrayBuffer(8);
    var u8 = new Uint8Array(buf);
    for (var k = 0; k < 8; k++) u8[k] = bytes[offset + k];
    if (u8[0] & 0x80) {
      u8[0] &= 0x7F;
    } else {
      for (var m = 0; m < 8; m++) u8[m] = ~u8[m] & 0xFF;
    }
    return new DataView(buf).getFloat64(0, false);
  }

  // Logical: 1 byte. 0x00 = null, 0x80 = false, 0x81 = true.
  function parseLogical(bytes, offset) {
    var b = bytes[offset];
    if (b === 0) return null;
    return (b & 0x7F) === 1;
  }

  // ─── String helpers ───────────────────────────────────────────────
  function asciiDecode(bytes, start, end) {
    var s = '';
    for (var i = start; i < end; i++) {
      var c = bytes[i];
      if (c >= 0x20 && c < 0x7F) s += String.fromCharCode(c);
      else if (c === 0) break;
      else s += ' '; // replace non-printable with space
    }
    return s;
  }

  function readNulString(bytes, offset, max) {
    var end = offset;
    var stop = Math.min(offset + max, bytes.length);
    while (end < stop && bytes[end] !== 0) end++;
    return asciiDecode(bytes, offset, end);
  }

  // ─── Export ───────────────────────────────────────────────────────
  global.Paradox = {
    parseDb: parseParadoxDb
  };

})(typeof window !== 'undefined' ? window : this);
