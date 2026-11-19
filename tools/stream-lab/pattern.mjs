// Frame-identity pattern shared by the sender page (draws it) and the
// analyzer (reads it back from decoded frames). This file is the contract
// between the two: change both sides together.
//
// Two horizontal bands at the top of the frame, each CELLS equal-width cells,
// black = 0 / white = 1, least significant bit in the leftmost cell:
//   band 0: frame counter (24 bits) + check byte (8 bits)
//   band 1: sender wall clock, Date.now() low 32 bits (for end-to-end latency)
// Cells are large (1/32 of the width, 1/12 of the height) so the pattern
// survives encoder blur and the analyzer's downscale; the analyzer samples
// each cell's centre with a fixed threshold.

export const CELLS = 32;
export const BANDS = 2;
export const BAND_HEIGHT_FRAC = 1 / 12;
export const THRESHOLD = 128;

export function checkByte(counter) {
  return (counter ^ (counter >>> 8) ^ (counter >>> 16) ^ 0xa5) & 0xff;
}

/** Bits per band (array of BANDS arrays of CELLS 0/1 values). */
export function encodeBands(counter, clockMs) {
  const c = counter & 0xffffff;
  const word0 = (c | (checkByte(c) << 24)) >>> 0;
  const word1 = clockMs >>> 0;
  return [word0, word1].map(word => Array.from({ length: CELLS }, (_, i) => (word >>> i) & 1));
}

/** Draw the bands onto a 2D canvas context covering a width x height frame. */
export function drawBands(ctx, width, height, counter, clockMs) {
  const bands = encodeBands(counter, clockMs);
  const cellW = width / CELLS;
  const bandH = height * BAND_HEIGHT_FRAC;
  bands.forEach((bits, b) => {
    bits.forEach((bit, i) => {
      ctx.fillStyle = bit ? '#fff' : '#000';
      ctx.fillRect(Math.floor(i * cellW), Math.floor(b * bandH), Math.ceil(cellW), Math.ceil(bandH));
    });
  });
}

/**
 * Read the bands back. `luma(x, y)` returns 0..255 for a pixel of a
 * width x height frame. Returns `{counter, clockLow32}` or `null` when the
 * check byte doesn't match (corrupted / not our pattern).
 */
export function decodeBands(luma, width, height) {
  const cellW = width / CELLS;
  const bandH = height * BAND_HEIGHT_FRAC;
  const words = [];
  for (let b = 0; b < BANDS; b++) {
    const y = Math.min(height - 1, Math.floor((b + 0.5) * bandH));
    let word = 0;
    for (let i = 0; i < CELLS; i++) {
      const x = Math.min(width - 1, Math.floor((i + 0.5) * cellW));
      if (luma(x, y) >= THRESHOLD) word |= 1 << i;
    }
    words.push(word >>> 0);
  }
  const counter = words[0] & 0xffffff;
  if (words[0] >>> 24 !== checkByte(counter)) return null;
  return { counter, clockLow32: words[1] };
}

/** Recover a full Date.now() value from its low 32 bits near `nowMs`. */
export function expandClock(clockLow32, nowMs) {
  const span = 2 ** 32;
  const base = nowMs - (nowMs % span);
  let t = base + clockLow32;
  if (t - nowMs > span / 2) t -= span;
  else if (nowMs - t > span / 2) t += span;
  return t;
}
