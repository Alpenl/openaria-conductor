//! Audio continuity in ALSA MONOTONIC_RAW, placement in host MONOTONIC.
//! A row is [sample, alsa_raw, observed_monotonic, raw_before, raw_after].
//! The last three values bracket one clock-domain observation; no NTP
//! correction is removed from the video/IMU timeline.

pub(crate) type Anchor = [u64; 5];

pub(crate) struct Model {
    pub(crate) start_ns: f64,
    pub(crate) end_ns: f64,
    pub(crate) residual_ns: f64,
}

pub(crate) fn mapped_ns(anchor: &Anchor) -> Result<u64, String> {
    let [position, raw, mono, before, after] = *anchor;
    if position == 0
        || raw == 0
        || mono == 0
        || raw > before
        || before > after
        || after - before > 1_000_000
        || after - raw > 20_000_000
    {
        return Err("audio clock bridge is stale or uncertain".into());
    }
    let midpoint = before + (after - before) / 2;
    // raw <= midpoint, so subtract the elapsed raw time without overflow.
    mono.checked_sub(midpoint - raw)
        .filter(|value| *value > 0)
        .ok_or_else(|| "invalid mapped audio clock".into())
}

fn sample_time(points: &[[u64; 2]], sample: u64) -> f64 {
    let index = points
        .partition_point(|point| point[0] <= sample)
        .clamp(1, points.len() - 1);
    let first = points[index - 1];
    let last = points[index];
    first[1] as f64
        + (sample as f64 - first[0] as f64) * (last[1] - first[1]) as f64
            / (last[0] - first[0]) as f64
}

pub(crate) fn evaluate(
    anchors: &[Anchor],
    rate: u32,
    samples: u64,
    period: u64,
    buffer: u64,
) -> Result<Model, String> {
    if anchors.len() < 2 || rate == 0 || samples == 0 || period == 0 || period > buffer {
        return Err("invalid raw audio clock domain".into());
    }
    let points = anchors
        .iter()
        .map(|anchor| mapped_ns(anchor).map(|mono| [anchor[0], mono]))
        .collect::<Result<Vec<_>, _>>()?;
    for (pair, mapped) in anchors.windows(2).zip(points.windows(2)) {
        let (first, last) = (pair[0], pair[1]);
        if last[0] <= first[0]
            || last[1] <= first[1]
            || last[2] <= first[2]
            || last[3] <= first[4]
            || mapped[1][1] <= mapped[0][1]
        {
            return Err("nonmonotonic raw audio clock or bridge".into());
        }
        if last[0] - first[0] > u64::from(rate).saturating_add(buffer) {
            return Err("raw audio clock evidence has missing intervals".into());
        }
        let raw_span = (last[3] - first[3]) as f64
            + ((last[4] - last[3]) as f64 - (first[4] - first[3]) as f64) / 2.0;
        let uncertainty = ((last[4] - last[3]) as f64 + (first[4] - first[3]) as f64) / 2.0;
        if ((last[2] - first[2]) as f64 - raw_span).abs() > raw_span * 0.01 + uncertainty {
            return Err("audio clock bridge rate outside supported range".into());
        }
    }
    let (first, last) = (anchors[0], anchors[anchors.len() - 1]);
    if first[0] > buffer.saturating_add(period)
        || last[0] < samples
        || last[0] > samples.saturating_add(buffer)
    {
        return Err("raw audio anchors do not cover captured samples".into());
    }
    let slope = (last[1] - first[1]) as f64 / (last[0] - first[0]) as f64;
    let actual_rate = 1e9 / slope;
    let residual_ns = anchors
        .iter()
        .map(|row| ((row[1] - first[1]) as f64 - (row[0] - first[0]) as f64 * slope).abs())
        .fold(0.0_f64, f64::max);
    if (actual_rate / f64::from(rate) - 1.0).abs() > 0.01 || residual_ns > 20_000_000.0 {
        return Err(format!(
            "raw audio clock discontinuity: rate={actual_rate}, residual_ns={residual_ns}"
        ));
    }
    Ok(Model {
        start_ns: sample_time(&points, 0),
        end_ns: sample_time(&points, samples),
        residual_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Vec<Anchor> {
        (0_u64..=600)
            .map(|second| {
                let sample = second * 48000 + 48;
                let raw = 2_000_000_000 + sample * 1_000_000_000 / 48000;
                let before = raw + 10_000;
                let after = raw + 30_000;
                let elapsed = raw + 20_000 - 2_000_000_000;
                let correction =
                    elapsed.saturating_sub(100_000_000_000).min(300_000_000_000) / 2000;
                [
                    sample,
                    raw,
                    1_100_000_000 + elapsed - correction,
                    before,
                    after,
                ]
            })
            .collect()
    }

    #[test]
    fn ordinary_slew_keeps_sample_continuity_and_non_affine_placement() {
        let model = evaluate(&fixture(), 48000, 600 * 48000, 1024, 8192).unwrap();
        assert!(model.residual_ns < 1.0);
        assert!((model.start_ns - 1_100_000_000.0).abs() <= 2.0);
        assert!((model.end_ns - 600_950_000_000.0).abs() <= 2.0);
    }

    #[test]
    fn real_gap_is_not_hidden_by_clock_bridge() {
        let mut anchors = fixture();
        for anchor in &mut anchors[300..] {
            for time in &mut anchor[1..] {
                *time += 60_000_000;
            }
        }
        assert!(
            evaluate(&anchors, 48000, 600 * 48000, 1024, 8192)
                .err()
                .unwrap()
                .contains("raw audio clock discontinuity")
        );
    }

    #[test]
    fn uncertain_stale_nonmonotonic_or_missing_evidence_is_rejected() {
        for column in 0..5 {
            let mut anchors = fixture();
            anchors[100][column] = 1;
            assert!(evaluate(&anchors, 48000, 600 * 48000, 1024, 8192).is_err());
        }
        let mut anchors = fixture();
        anchors[100][4] += 2_000_000;
        assert!(evaluate(&anchors, 48000, 600 * 48000, 1024, 8192).is_err());
        let mut anchors = fixture();
        anchors.drain(100..110);
        assert!(evaluate(&anchors, 48000, 600 * 48000, 1024, 8192).is_err());
    }
}
