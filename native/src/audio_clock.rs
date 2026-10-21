//! Audio continuity in ALSA MONOTONIC_RAW, placement in host MONOTONIC.
//! A row is [sample, alsa_raw, observed_monotonic, raw_before, raw_after].
//! The last three values bracket one clock-domain observation; no NTP
//! correction is removed from the video/IMU timeline.

pub(crate) type Anchor = [u64; 5];

/// Retain one host observation for each ALSA sample/time pair. USB reads can
/// observe the same hardware point again with a later, individually validated
/// host bracket; it is not a new point and must not duplicate a final anchor.
pub(crate) fn retain_same_point(previous: Option<Anchor>, next: Anchor) -> Anchor {
    match previous {
        Some(first) if first[..2] == next[..2] => first,
        _ => next,
    }
}

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

/// A delayed clock observation is not itself evidence of lost PCM. Refresh the
/// ALSA position/timestamp together with the host bracket, at most three times.
/// Invalid clocks and ALSA errors still fail immediately. Every rejected row is
/// reported before returning or retrying; continuity is checked independently.
pub(crate) fn observe_anchor(
    mut sample: impl FnMut() -> Result<Anchor, crate::audio::AudioError>,
    mut rejected: impl FnMut(Anchor),
) -> Result<(Anchor, u64), crate::audio::AudioError> {
    for attempt in 1..=3 {
        let anchor = sample()?;
        match mapped_ns(&anchor) {
            Ok(mapped) => return Ok((anchor, mapped)),
            Err(message) => {
                rejected(anchor);
                let [position, raw, mono, before, after] = anchor;
                let retryable = position > 0
                    && raw > 0
                    && mono > 0
                    && raw <= before
                    && before <= after
                    && (after - before > 1_000_000 || after - raw > 20_000_000);
                if !retryable || attempt == 3 {
                    return Err(crate::audio::AudioError::new(
                        "audio_failed",
                        format!("{message}; attempt={attempt}; anchor={anchor:?}"),
                    ));
                }
            }
        }
    }
    unreachable!("bounded clock sampling always returns")
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

    #[test]
    fn delayed_observation_refreshes_alsa_position_and_clock_pair() {
        for wide_bracket in [false, true] {
            let good = fixture()[1];
            let mut delayed = fixture()[0];
            if wide_bracket {
                delayed[4] += 2_000_000;
            } else {
                for value in &mut delayed[2..] {
                    *value += 30_000_000;
                }
            }
            let mut observations = [delayed, good].into_iter();
            let mut rejected = Vec::new();
            let (actual, mapped) = observe_anchor(
                || Ok(observations.next().expect("unexpected sampling")),
                |row| rejected.push(row),
            )
            .unwrap();
            assert_eq!(actual, good);
            assert_eq!(mapped, mapped_ns(&good).unwrap());
            assert_eq!(rejected, [delayed]);
        }
    }

    #[test]
    fn persistent_observation_delay_is_bounded_and_retains_last_row() {
        let mut delayed = fixture()[0];
        delayed[4] += 2_000_000;
        let mut calls = 0;
        let mut rejected = Vec::new();
        let error = observe_anchor(
            || {
                calls += 1;
                Ok(delayed)
            },
            |row| rejected.push(row),
        )
        .unwrap_err();
        assert_eq!(calls, 3);
        assert_eq!(rejected, [delayed; 3]);
        assert!(error.message.contains("attempt=3"));
        assert!(error.message.contains(&format!("anchor={delayed:?}")));
    }

    #[test]
    fn invalid_clock_and_pcm_error_are_not_retried() {
        let mut future = fixture()[0];
        future[1] = future[4] + 1;
        let mut calls = 0;
        let mut rejected = Vec::new();
        assert!(
            observe_anchor(
                || {
                    calls += 1;
                    Ok(future)
                },
                |row| rejected.push(row),
            )
            .is_err()
        );
        assert_eq!(calls, 1);
        assert_eq!(rejected, [future]);
        let failure = crate::audio::AudioError::new("audio_failed", "ALSA overrun");
        calls = 0;
        let error = observe_anchor(
            || {
                calls += 1;
                Err(failure.clone())
            },
            |_| panic!("ALSA failure has no clock row"),
        )
        .unwrap_err();
        assert_eq!(error, failure);
        assert_eq!(calls, 1);
    }
}

#[cfg(test)]
mod repeated_point_tests {
    use super::retain_same_point;

    #[test]
    fn refreshed_bridge_retains_original_hardware_point() {
        let first = [
            4800,
            1_000_000_000,
            2_000_000_100,
            1_000_000_050,
            1_000_000_150,
        ];
        let later = [
            4800,
            1_000_000_000,
            2_000_100_100,
            1_000_100_050,
            1_000_100_150,
        ];
        assert!(super::mapped_ns(&first).is_ok());
        assert!(super::mapped_ns(&later).is_ok());
        assert_eq!(retain_same_point(Some(first), later), first);
        assert_eq!(retain_same_point(None, later), later);
    }

    #[test]
    fn distinct_and_regressed_hardware_points_remain_for_validation() {
        let first = [
            4800,
            1_000_000_000,
            2_000_000_100,
            1_000_000_050,
            1_000_000_150,
        ];
        for (sample, raw) in [
            (4801, 1_000_001_000),
            (4799, 999_999_000),
            (4800, 1_000_001_000),
            (4801, 1_000_000_000),
        ] {
            let next = [sample, raw, 2_000_100_100, 1_000_100_050, 1_000_100_150];
            assert_eq!(retain_same_point(Some(first), next), next);
        }
    }
}
