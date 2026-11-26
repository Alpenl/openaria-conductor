use crate::audio;
use crate::hardware::{imu, metrics, v4l2};
use crate::session::{
    active_take, capture_runtime, recording, session_io, session_store, stereo_encoder,
};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyList, PySequence, PySequenceMethods};

pub(crate) fn focus_status_dict(py: Python<'_>, status: v4l2::FocusStatus) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("schema", "ylx.camera-focus.v1")?;
    result.set_item("value", status.value)?;
    result.set_item("minimum", status.minimum)?;
    result.set_item("maximum", status.maximum)?;
    result.set_item("step", status.step)?;
    result.set_item("default", status.default_value)?;
    result.set_item("auto_supported", status.auto_supported)?;
    result.set_item("auto_enabled", status.auto_enabled)?;
    Ok(result.unbind())
}

pub(crate) fn exposure_config_dict(
    py: Python<'_>,
    config: v4l2::ExposureConfig,
) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);
    result.set_item("auto_exposure", config.auto_exposure)?;
    if let Some(value) = config.exposure_time_absolute {
        result.set_item("exposure_time_absolute", value)?;
    }
    Ok(result.unbind())
}

pub(crate) fn imu_observation_dict(
    py: Python<'_>,
    result: &imu::ImuObservation,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("dropped_samples", result.dropped_samples)?;
    // No independent IMU sample counter is present in the camera's payload.
    value.set_item("missed_packets_estimate_available", false)?;
    let samples = PyList::empty(py);
    for sample in &result.samples {
        let item = PyDict::new(py);
        item.set_item("sequence", sample.sequence)?;
        item.set_item("packet_sequence", sample.packet_sequence)?;
        item.set_item("sample_index", sample.sample_index)?;
        item.set_item("device_timestamp_raw", sample.device_timestamp_raw)?;
        item.set_item("device_ticks", sample.device_ticks)?;
        item.set_item("host_read_start_ns", sample.host_read_start_ns)?;
        item.set_item("host_read_end_ns", sample.host_read_end_ns)?;
        item.set_item("host_monotonic_ns", sample.host_monotonic_ns)?;

        let raw = PyDict::new(py);
        raw.set_item(
            "accelerometer",
            vec![
                sample.accelerometer.x,
                sample.accelerometer.y,
                sample.accelerometer.z,
            ],
        )?;
        raw.set_item(
            "gyroscope",
            vec![sample.gyroscope.x, sample.gyroscope.y, sample.gyroscope.z],
        )?;
        item.set_item("raw", raw)?;

        let sync = PyDict::new(py);
        sync.set_item("offset_ns", sample.sync_offset_ns)?;
        sync.set_item("residual_ns", sample.sync_residual_ns)?;
        sync.set_item("quality", sample.sync_quality)?;
        item.set_item("sync", sync)?;

        samples.append(item)?;
    }
    value.set_item("samples", samples)?;
    Ok(value.unbind())
}

pub(crate) fn metrics_snapshot_dict(
    py: Python<'_>,
    snapshot: &metrics::MetricsSnapshot,
) -> PyResult<Py<PyDict>> {
    let result = PyDict::new(py);

    let stages = PyList::empty(py);
    for stage in &snapshot.stages {
        let item = PyDict::new(py);
        item.set_item("name", &stage.name)?;
        item.set_item("samples", stage.samples)?;
        item.set_item("p50_ns", stage.p50_ns)?;
        item.set_item("p95_ns", stage.p95_ns)?;
        item.set_item("total_ns", stage.total_ns)?;
        stages.append(item)?;
    }
    result.set_item("stages", stages)?;

    let copies = PyList::empty(py);
    for copy in &snapshot.copies {
        let item = PyDict::new(py);
        item.set_item("name", &copy.name)?;
        item.set_item("count", copy.count)?;
        item.set_item("bytes_total", copy.bytes_total)?;
        copies.append(item)?;
    }
    result.set_item("copies", copies)?;

    let payloads = PyList::empty(py);
    for payload in &snapshot.payloads {
        let item = PyDict::new(py);
        item.set_item("name", &payload.name)?;
        item.set_item("acquired", payload.acquired)?;
        item.set_item("live", payload.live)?;
        item.set_item("live_bytes", payload.live_bytes)?;
        item.set_item("peak_live", payload.peak_live)?;
        item.set_item("peak_bytes", payload.peak_bytes)?;
        payloads.append(item)?;
    }
    result.set_item("payloads", payloads)?;

    let queue = PyDict::new(py);
    queue.set_item("capacity", snapshot.queue.capacity)?;
    queue.set_item("peak_depth", snapshot.queue.peak_depth)?;
    queue.set_item("rejected", snapshot.queue.rejected)?;
    result.set_item("queue", queue)?;

    let loss = PyDict::new(py);
    loss.set_item("source_gap", snapshot.loss.source_gap)?;
    loss.set_item("queue_rejected", snapshot.loss.queue_rejected)?;
    loss.set_item("write_failure", snapshot.loss.write_failure)?;
    loss.set_item("unknown_gap", snapshot.loss.unknown_gap)?;
    result.set_item("loss", loss)?;

    Ok(result.unbind())
}

fn audio_result_dict(py: Python<'_>, result: &audio::AudioRecordingResult) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("device", &result.device)?;
    value.set_item("codec", result.codec)?;
    value.set_item("container", result.container)?;
    value.set_item("sample_rate_hz", result.sample_rate_hz)?;
    value.set_item("channels", result.channels)?;
    value.set_item("sample_format", result.sample_format)?;
    value.set_item("sample_count", result.sample_count)?;
    value.set_item("capture_clock_json", result.capture_clock.to_string())?;
    value.set_item("started_monotonic_ns", result.started_monotonic_ns)?;
    value.set_item("stopped_monotonic_ns", result.stopped_monotonic_ns)?;
    let segments = PyList::empty(py);
    for segment in &result.segments {
        let item = PyDict::new(py);
        item.set_item("index", segment.index)?;
        item.set_item("path", &segment.relative_path)?;
        item.set_item("start_sample", segment.start_sample)?;
        item.set_item("end_sample", segment.end_sample)?;
        item.set_item("start_time_seconds", segment.start_time_seconds)?;
        item.set_item("end_time_seconds", segment.end_time_seconds)?;
        segments.append(item)?;
    }
    value.set_item("segments", segments)?;
    Ok(value.unbind())
}

fn audio_snapshot_dict(
    py: Python<'_>,
    snapshot: &audio::AudioRecordingSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("sample_count", snapshot.sample_count)?;
    value.set_item("bytes_written", snapshot.bytes_written)?;
    value.set_item("segment_count", snapshot.segment_count)?;
    Ok(value.unbind())
}

fn active_take_snapshot_dict(
    py: Python<'_>,
    snapshot: &active_take::ActiveTakeSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("session_id", &snapshot.session_id)?;
    value.set_item("frame_domain", snapshot.frame_domain)?;
    value.set_item("frames_written", snapshot.frames_written)?;
    value.set_item("bytes_written", snapshot.bytes_written)?;
    value.set_item("dropped_frames", snapshot.dropped_frames)?;
    value.set_item("pending_frames", snapshot.pending_frames)?;
    let drop_events = PyList::empty(py);
    for event in &snapshot.drop_events {
        let item = PyDict::new(py);
        item.set_item("start_frame", event.start_frame)?;
        item.set_item("end_frame", event.end_frame)?;
        item.set_item("at_time_seconds", event.at_time_seconds)?;
        item.set_item("reason", event.reason)?;
        item.set_item("dropped", event.dropped)?;
        drop_events.append(item)?;
    }
    value.set_item("drop_events", drop_events)?;
    Ok(value.unbind())
}

fn recording_sink_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::RecordingSinkSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("bytes_written", snapshot.bytes_written)?;
    value.set_item("frames_written", snapshot.frames_written)?;
    value.set_item("imu_samples_written", snapshot.imu_samples_written)?;
    value.set_item("frames_indexed", snapshot.frames_indexed)?;
    value.set_item("imu_samples_indexed", snapshot.imu_samples_indexed)?;
    value.set_item("index_pending_bytes", snapshot.index_pending_bytes)?;
    value.set_item("index_peak_bytes", snapshot.index_peak_bytes)?;
    value.set_item("index_max_write_ns", snapshot.index_max_write_ns)?;
    let artifacts = PyDict::new(py);
    for artifact in &snapshot.artifacts {
        let item = PyDict::new(py);
        item.set_item("role", artifact.role)?;
        item.set_item("path", artifact.relative_path)?;
        item.set_item("bytes", artifact.bytes)?;
        item.set_item("sha256", &artifact.sha256)?;
        let identity = PyDict::new(py);
        identity.set_item("device", artifact.identity.device)?;
        identity.set_item("inode", artifact.identity.inode)?;
        identity.set_item("size", artifact.identity.size)?;
        identity.set_item("mtime_ns", artifact.identity.mtime_ns)?;
        item.set_item("identity", identity)?;
        artifacts.set_item(artifact.role, item)?;
    }
    value.set_item("artifacts", artifacts)?;
    Ok(value.unbind())
}

fn capture_runtime_error_dict(
    py: Python<'_>,
    error: &capture_runtime::RuntimeError,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("code", error.code.as_ref())?;
    value.set_item("message", &error.message)?;
    Ok(value.unbind())
}

pub(crate) fn session_transaction_snapshot_dict(
    py: Python<'_>,
    snapshot: &session_store::TransactionSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("state", snapshot.state)?;
    value.set_item(
        "active_take",
        active_take_snapshot_dict(py, &snapshot.active_take)?,
    )?;
    value.set_item("sink", recording_sink_snapshot_dict(py, &snapshot.sink)?)?;
    match &snapshot.audio {
        Some(audio) => value.set_item("audio", audio_snapshot_dict(py, audio)?)?,
        None => value.set_item("audio", py.None())?,
    }
    value.set_item("submitted_frames", snapshot.submitted_frames)?;
    match &snapshot.audio_failure {
        Some(error) => {
            let failure = PyDict::new(py);
            failure.set_item("code", error.code)?;
            failure.set_item("message", &error.message)?;
            value.set_item("audio_failure", failure)?;
        }
        None => value.set_item("audio_failure", py.None())?,
    }
    let encoder_stats = PyDict::new(py);
    for (name, count) in &snapshot.encoder_stats {
        encoder_stats.set_item(name, count)?;
    }
    value.set_item("encoder_stats", encoder_stats)?;
    match &snapshot.encoder_failure {
        Some(error) => {
            let failure = PyDict::new(py);
            failure.set_item("code", &error.code)?;
            failure.set_item("message", &error.message)?;
            value.set_item("encoder_failure", failure)?;
        }
        None => value.set_item("encoder_failure", py.None())?,
    }
    Ok(value.unbind())
}

pub(crate) fn recording_outcome_dict(
    py: Python<'_>,
    outcome: &session_store::RecordingOutcome,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item(
        "active_take",
        active_take_snapshot_dict(py, &outcome.active_take)?,
    )?;
    value.set_item("sink", recording_sink_snapshot_dict(py, &outcome.sink)?)?;
    match &outcome.audio {
        Some(audio) => value.set_item("audio", audio_result_dict(py, audio)?)?,
        None => value.set_item("audio", py.None())?,
    }
    value.set_item(
        "segments",
        stereo_encoder_segment_list(py, &outcome.segments)?,
    )?;
    let encoder_stats = PyDict::new(py);
    for (name, count) in &outcome.encoder_stats {
        encoder_stats.set_item(name, count)?;
    }
    value.set_item("encoder_stats", encoder_stats)?;
    value.set_item(
        "planner",
        recording_segment_planner_snapshot_dict(py, &outcome.planner)?,
    )?;
    Ok(value.unbind())
}

pub(crate) fn capture_runtime_snapshot_dict(
    py: Python<'_>,
    snapshot: &capture_runtime::RuntimeSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("running", snapshot.running)?;
    value.set_item("recording_present", snapshot.recording_present)?;
    value.set_item("recording_active", snapshot.recording_active)?;
    value.set_item("inflight_frames", snapshot.inflight_frames)?;
    value.set_item("observed_frames", snapshot.observed_frames)?;
    value.set_item("failure_reported", snapshot.failure_reported)?;
    match &snapshot.terminal_error {
        Some(error) => value.set_item("terminal_error", capture_runtime_error_dict(py, error)?)?,
        None => value.set_item("terminal_error", py.None())?,
    }
    match &snapshot.last_preview_error {
        Some(error) => {
            value.set_item("last_preview_error", capture_runtime_error_dict(py, error)?)?
        }
        None => value.set_item("last_preview_error", py.None())?,
    }
    Ok(value.unbind())
}

pub(crate) fn recording_segment_boundary_dict(
    py: Python<'_>,
    boundary: &recording::SegmentBoundary,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("frame", boundary.frame)?;
    value.set_item("time_seconds", boundary.time_seconds)?;
    Ok(value.unbind())
}

fn recording_segment_planner_snapshot_dict(
    py: Python<'_>,
    snapshot: &recording::SegmentPlannerSnapshot,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("segment_frames", snapshot.segment_frames)?;
    value.set_item("frames_written", snapshot.frames_written)?;
    value.set_item("segment_count", snapshot.segment_count)?;
    value.set_item("covered_frames", snapshot.covered_frames)?;
    value.set_item("boundary_count", snapshot.boundary_count)?;
    Ok(value.unbind())
}

pub(crate) fn stereo_encoder_segment_list(
    py: Python<'_>,
    segments: &[stereo_encoder::SegmentEvent],
) -> PyResult<Py<PyList>> {
    let value = PyList::empty(py);
    for segment in segments {
        let item = PyDict::new(py);
        item.set_item("index", segment.index)?;
        item.set_item("start_frame", segment.start_frame)?;
        item.set_item("end_frame", segment.end_frame)?;
        item.set_item("left_path", &segment.left_path)?;
        item.set_item("left_bytes", segment.left_bytes)?;
        item.set_item("right_path", &segment.right_path)?;
        item.set_item("right_bytes", segment.right_bytes)?;
        value.append(item)?;
    }
    Ok(value.unbind())
}

pub(crate) fn file_identity_dict(
    py: Python<'_>,
    identity: &session_io::FileIdentity,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("device", identity.device)?;
    value.set_item("inode", identity.inode)?;
    value.set_item("size", identity.size)?;
    value.set_item("modified_ns", identity.modified_ns)?;
    value.set_item("nlink", identity.nlink)?;
    Ok(value.unbind())
}

pub(crate) fn expected_artifact_identities(
    raw: &Bound<'_, PyDict>,
) -> PyResult<Vec<session_io::ExpectedArtifactIdentity>> {
    let mut identities = Vec::with_capacity(raw.len());
    for (path, identity) in raw.iter() {
        let path = path.extract::<String>()?;
        let identity = identity.downcast::<PySequence>()?;
        if identity.len()? != 4 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "artifact_invalid: artifact identity must be a 4-item tuple",
            ));
        }
        let device = strict_u64_identity_field(&identity.get_item(0)?, "device")?;
        let inode = strict_u64_identity_field(&identity.get_item(1)?, "inode")?;
        let size = strict_u64_identity_field(&identity.get_item(2)?, "size")?;
        let modified_ns = strict_i64_identity_field(&identity.get_item(3)?, "modified_ns")?;
        identities.push(session_io::ExpectedArtifactIdentity {
            path,
            identity: session_io::FileIdentity {
                device,
                inode,
                size,
                modified_ns,
                nlink: 1,
            },
        });
    }
    Ok(identities)
}

fn strict_u64_identity_field(value: &Bound<'_, PyAny>, field: &str) -> PyResult<u64> {
    if value.is_instance_of::<PyBool>() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be an integer"
        )));
    }
    value.extract::<u64>().map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be a non-negative integer: {error}"
        ))
    })
}

fn strict_i64_identity_field(value: &Bound<'_, PyAny>, field: &str) -> PyResult<i64> {
    if value.is_instance_of::<PyBool>() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be an integer"
        )));
    }
    value.extract::<i64>().map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "artifact_invalid: artifact identity {field} must be an integer: {error}"
        ))
    })
}

pub(crate) fn file_digest_dict(
    py: Python<'_>,
    digest: &session_io::FileDigest,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("sha256", &digest.sha256)?;
    value.set_item("identity", file_identity_dict(py, &digest.identity)?)?;
    Ok(value.unbind())
}

pub(crate) fn device_session_seal_result_dict(
    py: Python<'_>,
    result: &session_io::DeviceSessionSealResult,
) -> PyResult<Py<PyDict>> {
    let value = PyDict::new(py);
    value.set_item("manifest_sha256", &result.manifest_sha256)?;
    value.set_item("artifact_count", result.artifact_count)?;
    value.set_item("manifest_bytes", result.manifest_bytes)?;
    Ok(value.unbind())
}
