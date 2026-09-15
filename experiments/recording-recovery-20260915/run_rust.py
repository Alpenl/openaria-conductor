"""Run the real failed-recording retry path in an isolated source snapshot."""

import io
import json
import os
import subprocess
import sysconfig
import tarfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
OUT = Path(__file__).parent
SNAPSHOT = OUT / "source-snapshot"
SNAPSHOT.mkdir(exist_ok=True)
archive = subprocess.check_output(["git", "archive", "HEAD"], cwd=REPO)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
    tar.extractall(SNAPSHOT, filter="data")

path = SNAPSHOT / "native/src/capture_runtime.rs"
source = path.read_text()
start = source.index(
    "    #[test]\n    fn prepare_recording_reaps_failed_imu_worker_and_allows_retry()"
)
end = source.index("    #[test]", start + 12)
probe = source[start:end]
probe = probe.replace(
    "fn prepare_recording_reaps_failed_imu_worker_and_allows_retry()",
    "fn ablation_recording_retry()",
)
probe = probe.replace(
    "            runtime.prepare_recording(Duration::from_secs(1)).unwrap();\n"
    "            assert!(runtime.imu_worker.lock().unwrap().is_none());",
    """            let ablated = std::env::var("OPENARIA_ABLATE_RETRY").is_ok();
            if ablated {
                // Exact old lib.rs gating: failure clears recording_present,
                // although the IMU worker handle still exists.
                if runtime.snapshot().unwrap().recording_present {
                    runtime.stop_recording(Duration::from_secs(1)).unwrap();
                }
            } else {
                runtime.prepare_recording(Duration::from_secs(1)).unwrap();
            }
            let stale_worker = runtime.imu_worker.lock().unwrap().is_some();""",
)
probe = probe.replace(
    "            let snapshot = runtime\n                .start_recording(",
    "            let outcome = runtime\n                .start_recording(",
)
tail_start = probe.index("                .unwrap();", probe.index("            let outcome"))
tail_end = probe.index("            let _ = fs::remove_dir_all(root);", tail_start)
probe = (
    probe[:tail_start]
    + """                ;
            let error = outcome.as_ref().err().map(|e| e.code).unwrap_or("");
            println!("ABLATION {}", serde_json::json!({
                "experiment": "recording_retry",
                "variant": if ablated {"old_conditional_cleanup"} else {"full"},
                "stale_worker_after_prepare": stale_worker,
                "retry_started": outcome.is_ok(),
                "error": error
            }));
            assert_eq!(outcome.is_ok(), !ablated);
            runtime.stop_recording(Duration::from_secs(1)).unwrap();
"""
    + probe[tail_end:]
)
assert "ablation_recording_retry" in probe and "let outcome" in probe
path.write_text(source[:end] + probe + source[end:])

env = dict(os.environ)
env["PYO3_PYTHON"] = str(REPO / ".venv/bin/python")
libdir = sysconfig.get_config_var("LIBDIR")
env["LD_LIBRARY_PATH"] = str(libdir) + ":" + env.get("LD_LIBRARY_PATH", "")
command = [
    "cargo",
    "test",
    "--manifest-path",
    str(SNAPSHOT / "Cargo.toml"),
    "--target-dir",
    str(REPO / "target"),
    "-p",
    "rp-ylx-native",
    "--lib",
    "ablation_recording_retry",
    "--no-run",
    "--message-format=json",
]
with (OUT / "rust-build.stderr").open("w") as errors:
    build = subprocess.run(
        command, env=env, stdout=subprocess.PIPE, stderr=errors, text=True, timeout=90
    )
(OUT / "rust-build.jsonl").write_text(build.stdout)
if build.returncode:
    print((OUT / "rust-build.stderr").read_text())
    raise SystemExit(build.returncode)
binary = None
for line in build.stdout.splitlines():
    try:
        value = json.loads(line)
    except ValueError:
        continue
    if value.get("reason") == "compiler-artifact" and value.get("executable"):
        binary = value["executable"]
assert binary
rows = []
for repeat in range(5):
    for ablated in [bool(repeat % 2), not bool(repeat % 2)]:
        trial_env = dict(env)
        if ablated:
            trial_env["OPENARIA_ABLATE_RETRY"] = "1"
        else:
            trial_env.pop("OPENARIA_ABLATE_RETRY", None)
        run = subprocess.run(
            [binary, "--exact", "capture_runtime::tests::ablation_recording_retry", "--nocapture"],
            env=trial_env,
            text=True,
            capture_output=True,
            timeout=10,
        )
        assert run.returncode == 0, run.stdout + run.stderr
        row = next(
            json.loads(line.split("ABLATION ", 1)[1])
            for line in run.stdout.splitlines()
            if "ABLATION " in line
        )
        row["repeat"] = repeat
        rows.append(row)
        print(json.dumps(row), flush=True)
(OUT / "rust-results.json").write_text(json.dumps(rows, indent=2) + "\n")
