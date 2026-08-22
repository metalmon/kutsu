//! Corpus loading for the offline harness: a `labels.toml` mapping WAV file
//! names to their true [`AmdClass`].

use std::collections::BTreeMap;
use std::path::Path;

use crate::amd::AmdClass;

#[derive(serde::Deserialize)]
struct LabelsFile {
    labels: BTreeMap<String, AmdClass>,
}

/// Parse a `labels.toml` (`[labels]` table of `"file.wav" = "human"`).
pub fn load_labels(path: &Path) -> anyhow::Result<BTreeMap<String, AmdClass>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
    let parsed: LabelsFile =
        toml::from_str(&text).map_err(|e| anyhow::anyhow!("parse {}: {e}", path.display()))?;
    Ok(parsed.labels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amd::AmdClass;

    #[test]
    fn loads_labels_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("labels.toml");
        std::fs::write(
            &path,
            "[labels]\n\"a.wav\" = \"human\"\n\"b.wav\" = \"hold\"\n",
        )
        .unwrap();
        let labels = load_labels(&path).unwrap();
        assert_eq!(labels.get("a.wav"), Some(&AmdClass::Human));
        assert_eq!(labels.get("b.wav"), Some(&AmdClass::Hold));
        assert_eq!(labels.len(), 2);
    }
}
