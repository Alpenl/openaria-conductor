//! Embedded offline Web Console assets (`index.html`, `app.js`, `styles.css`) compiled into
//! `miro-ego` from `crates/miro-ego-cli/web/` and verified against `web/assets.json`.

use bytes::Bytes;
use miro_ego_api::StaticAsset;
use std::collections::HashMap;

pub(super) const EMBEDDED_INDEX_HTML: &[u8] = include_bytes!("../../web/index.html");
pub(super) const EMBEDDED_APP_JS: &[u8] = include_bytes!("../../web/app.js");
pub(super) const EMBEDDED_STYLES_CSS: &[u8] = include_bytes!("../../web/styles.css");
#[cfg(test)]
pub(super) const EMBEDDED_ASSETS_JSON: &str = include_str!("../../web/assets.json");

/// Build the immutable map of embedded Web Console assets served by `miro-ego-api`.
pub fn load_embedded_web_assets() -> HashMap<String, StaticAsset> {
    let mut assets = HashMap::with_capacity(3);
    assets.insert(
        "index.html".to_owned(),
        StaticAsset::new(
            "index.html",
            "text/html; charset=utf-8",
            Bytes::from_static(EMBEDDED_INDEX_HTML),
        ),
    );
    assets.insert(
        "app.js".to_owned(),
        StaticAsset::new(
            "app.js",
            "application/javascript; charset=utf-8",
            Bytes::from_static(EMBEDDED_APP_JS),
        ),
    );
    assets.insert(
        "styles.css".to_owned(),
        StaticAsset::new(
            "styles.css",
            "text/css; charset=utf-8",
            Bytes::from_static(EMBEDDED_STYLES_CSS),
        ),
    );
    assets
}

#[cfg(test)]
mod tests {
    use super::*;
    use miro_ego_core::ids::hex_encode;
    use serde::Deserialize;
    use sha2::{Digest, Sha256};

    #[derive(Debug, Deserialize)]
    struct WebAssetsManifest {
        schema: String,
        total_bytes: usize,
        files: Vec<WebAssetEntry>,
    }

    #[derive(Debug, Deserialize)]
    struct WebAssetEntry {
        path: String,
        bytes: usize,
        sha256: String,
        content_type: String,
    }

    #[test]
    fn embedded_web_assets_match_manifest_digests_and_sizes() {
        let manifest: WebAssetsManifest =
            serde_json::from_str(EMBEDDED_ASSETS_JSON).expect("parse web/assets.json");
        assert_eq!(manifest.schema, "miro-ego.web-assets.v1");
        assert!(
            manifest.total_bytes <= 512 * 1024,
            "total embedded web bundle ({}) exceeds 512 KiB budget",
            manifest.total_bytes
        );

        let loaded = load_embedded_web_assets();
        assert_eq!(loaded.len(), manifest.files.len());

        let mut sum_bytes = 0_usize;
        for entry in &manifest.files {
            let asset = loaded
                .get(&entry.path)
                .unwrap_or_else(|| panic!("missing embedded web asset {}", entry.path));
            assert_eq!(asset.bytes.len(), entry.bytes);
            assert_eq!(asset.content_type, entry.content_type);
            let actual_sha = hex_encode(Sha256::digest(&asset.bytes).as_slice());
            assert_eq!(actual_sha, entry.sha256);
            assert_eq!(asset.etag, format!("\"{}\"", entry.sha256));
            sum_bytes += asset.bytes.len();
        }
        assert_eq!(sum_bytes, manifest.total_bytes);
    }
}
