//! ITU-T G.711 companding (µ-law / a-law), ported from the public-domain
//! Sun/CCITT reference. Pure, no I/O.

use crate::sip::G711Kind;

const BIAS: i32 = 0x84;
const CLIP: i32 = 8159;
const SEG_UEND: [i32; 8] = [0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF];
const SEG_AEND: [i32; 8] = [0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF];

fn search(val: i32, table: &[i32; 8]) -> usize {
    for (i, &t) in table.iter().enumerate() {
        if val <= t {
            return i;
        }
    }
    table.len()
}

/// PCM16 -> µ-law.
pub fn encode_ulaw(pcm: i16) -> u8 {
    let mut pcm_val = (pcm as i32) >> 2; // 16-bit -> 14-bit
    let mask = if pcm_val < 0 {
        pcm_val = -pcm_val;
        0x7F
    } else {
        0xFF
    };
    if pcm_val > CLIP {
        pcm_val = CLIP;
    }
    pcm_val += BIAS >> 2;
    let seg = search(pcm_val, &SEG_UEND);
    if seg >= 8 {
        (0x7F ^ mask) as u8
    } else {
        let uval = (seg as i32) << 4 | ((pcm_val >> (seg + 1)) & 0xF);
        (uval ^ mask) as u8
    }
}

/// µ-law -> PCM16.
pub fn decode_ulaw(u: u8) -> i16 {
    let u = (!u) as i32;
    let mut t = ((u & 0x0F) << 3) + BIAS;
    t <<= (u & 0x70) >> 4;
    (if (u & 0x80) != 0 { BIAS - t } else { t - BIAS }) as i16
}

/// PCM16 -> a-law.
pub fn encode_alaw(pcm: i16) -> u8 {
    let mut pcm_val = (pcm as i32) >> 3; // 16-bit -> 13-bit
    let mask = if pcm_val >= 0 {
        0xD5
    } else {
        pcm_val = -pcm_val - 1;
        0x55
    };
    let seg = search(pcm_val, &SEG_AEND);
    if seg >= 8 {
        (0x7F ^ mask) as u8
    } else {
        let mut aval = (seg as i32) << 4;
        aval |= if seg < 2 {
            (pcm_val >> 1) & 0xF
        } else {
            (pcm_val >> seg) & 0xF
        };
        (aval ^ mask) as u8
    }
}

/// a-law -> PCM16.
pub fn decode_alaw(a: u8) -> i16 {
    let a = (a ^ 0x55) as i32;
    let mut t = (a & 0x0F) << 4;
    let seg = (a & 0x70) >> 4;
    match seg {
        0 => t += 8,
        1 => t += 0x108,
        _ => {
            t += 0x108;
            t <<= seg - 1;
        }
    }
    (if (a & 0x80) != 0 { t } else { -t }) as i16
}

/// Decode a G.711 payload to PCM16 samples.
pub fn decode(kind: G711Kind, payload: &[u8]) -> Vec<i16> {
    match kind {
        G711Kind::Ulaw => payload.iter().map(|&b| decode_ulaw(b)).collect(),
        G711Kind::Alaw => payload.iter().map(|&b| decode_alaw(b)).collect(),
    }
}

/// Encode PCM16 samples to a G.711 payload.
pub fn encode(kind: G711Kind, pcm: &[i16]) -> Vec<u8> {
    match kind {
        G711Kind::Ulaw => pcm.iter().map(|&s| encode_ulaw(s)).collect(),
        G711Kind::Alaw => pcm.iter().map(|&s| encode_alaw(s)).collect(),
    }
}

/// The byte that encodes digital silence for this codec.
pub fn silence_byte(kind: G711Kind) -> u8 {
    match kind {
        G711Kind::Ulaw => 0xFF,
        G711Kind::Alaw => 0xD5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sip::G711Kind;

    #[test]
    fn silence_anchors() {
        // Digital silence encodes to the canonical bytes and decodes back near zero.
        assert_eq!(encode_ulaw(0), 0xFF);
        assert_eq!(decode_ulaw(0xFF), 0);
        assert_eq!(silence_byte(G711Kind::Ulaw), 0xFF);

        assert_eq!(encode_alaw(0), 0xD5);
        assert!(decode_alaw(0xD5).abs() < 16); // a-law min step, ~silence
        assert_eq!(silence_byte(G711Kind::Alaw), 0xD5);
    }

    #[test]
    fn ulaw_decode_is_stable_under_reencode() {
        // decode->encode->decode must reproduce the decoded PCM for all 256 codes.
        for b in 0u8..=255 {
            let pcm = decode_ulaw(b);
            assert_eq!(decode_ulaw(encode_ulaw(pcm)), pcm, "ulaw byte {b:#04x}");
        }
    }

    #[test]
    fn alaw_decode_is_stable_under_reencode() {
        for b in 0u8..=255 {
            let pcm = decode_alaw(b);
            assert_eq!(decode_alaw(encode_alaw(pcm)), pcm, "alaw byte {b:#04x}");
        }
    }

    #[test]
    fn frame_helpers_roundtrip_length() {
        let pcm = [0i16, 100, -100, 5000, -5000];
        let enc = encode(G711Kind::Ulaw, &pcm);
        assert_eq!(enc.len(), pcm.len());
        let dec = decode(G711Kind::Ulaw, &enc);
        assert_eq!(dec.len(), pcm.len());
    }
}
