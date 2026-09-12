//! Storage domain records, field-compatible with the tc-storage Go reference
//! at `tools/storage-cli/internal/domain/domain.go` (and with the web app's
//! `src/storage/domain.ts`, from which the Go types were themselves ported).
//!
//! These are plain data-transfer structs: JSON field names are part of the
//! wire contract and are reproduced verbatim (camelCase, exact Go
//! `omitempty`/pointer semantics), so do not rename or reorder fields
//! without checking every producer/consumer of this format.
//!
//! Optionality mirrors the Go struct tags exactly:
//! - A Go field with `omitempty` becomes `Option<T>` with
//!   `#[serde(skip_serializing_if = "Option::is_none", default)]`: omitted
//!   from JSON when `None` (same as an empty Go string/nil pointer being
//!   dropped), and defaulted to `None` on deserialize when the key is
//!   missing (lenient decoding, matching `encoding/json`'s tolerant
//!   `Unmarshal`).
//! - `FolderRecord.ParentID` is a Go `*string` **without** `omitempty`, so it
//!   always appears in JSON -- as `null` when there is no parent. It is
//!   mapped to `Option<String>` with no `skip_serializing_if`; JSON `null`
//!   decodes to `None` natively (no `default` attribute needed for that).
//! - Required scalars (`id`, `name`, `createdAt`, ...) are plain types with
//!   no `default`: a bundle missing one of these is malformed and should
//!   fail to deserialize rather than silently produce a zero value.

use std::collections::HashMap;

/// A per-field last-write marker used for conflict resolution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VersionStamp {
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "nodeId")]
    pub node_id: String,
}

/// A folder in the local storage tree.
///
/// Mirrors Go's `domain.FolderRecord`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FolderRecord {
    pub id: String,
    pub name: String,
    /// `*string` without `omitempty` in Go: always present in JSON, `null`
    /// when there is no parent (i.e. this is a root folder).
    #[serde(rename = "parentId")]
    pub parent_id: Option<String>,
    #[serde(rename = "sortOrder", skip_serializing_if = "Option::is_none", default)]
    pub sort_order: Option<f64>,
    pub color: String,
    pub encrypted: bool,
    #[serde(rename = "shareEnabled")]
    pub share_enabled: bool,
    #[serde(rename = "sharedRoomId")]
    pub shared_room_id: String,
    #[serde(rename = "lastCid", skip_serializing_if = "Option::is_none", default)]
    pub last_cid: Option<String>,
    #[serde(
        rename = "lastSavedAt",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub last_saved_at: Option<String>,
    #[serde(
        rename = "lastSharedAt",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub last_shared_at: Option<String>,
    #[serde(rename = "deletedAt", skip_serializing_if = "Option::is_none", default)]
    pub deleted_at: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(
        rename = "fieldVersions",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub field_versions: Option<HashMap<String, VersionStamp>>,
}

/// A file in the local storage tree.
///
/// Mirrors Go's `domain.FileRecord`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileRecord {
    pub id: String,
    #[serde(rename = "folderId")]
    pub folder_id: String,
    #[serde(rename = "sortOrder", skip_serializing_if = "Option::is_none", default)]
    pub sort_order: Option<f64>,
    pub name: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    pub size: i64,
    #[serde(rename = "dataUrl", skip_serializing_if = "Option::is_none", default)]
    pub data_url: Option<String>,
    pub checksum: String,
    pub version: i32,
    pub starred: bool,
    #[serde(rename = "lastCid", skip_serializing_if = "Option::is_none", default)]
    pub last_cid: Option<String>,
    #[serde(
        rename = "lastShareCid",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub last_share_cid: Option<String>,
    #[serde(rename = "deletedAt", skip_serializing_if = "Option::is_none", default)]
    pub deleted_at: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(
        rename = "fieldVersions",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub field_versions: Option<HashMap<String, VersionStamp>>,
}

/// A single-file export/import bundle: one file plus its owning folder.
///
/// Mirrors Go's `domain.FileBundle`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileBundle {
    pub version: i32,
    #[serde(rename = "exportedAt")]
    pub exported_at: String,
    #[serde(rename = "originNode")]
    pub origin_node: String,
    pub folder: FolderRecord,
    pub file: FileRecord,
}

/// A folder export/import bundle: a folder, its descendant folders (if any),
/// and all contained files.
///
/// Mirrors Go's `domain.FolderBundle`. Decrypted and consumed by
/// `folder_share::fetch_folder_share` (`store folder-get`), which is the
/// only producer of a *networked* folder fetch -- the local-only store still
/// can't originate one itself.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FolderBundle {
    pub version: i32,
    #[serde(rename = "exportedAt")]
    pub exported_at: String,
    #[serde(rename = "originNode")]
    pub origin_node: String,
    pub folder: FolderRecord,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub folders: Option<Vec<FolderRecord>>,
    pub files: Vec<FileRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_folder() -> FolderRecord {
        FolderRecord {
            id: "folder-1".to_string(),
            name: "My Folder".to_string(),
            parent_id: None,
            sort_order: None,
            color: "#ff0000".to_string(),
            encrypted: true,
            share_enabled: true,
            shared_room_id: "room-abc".to_string(),
            last_cid: None,
            last_saved_at: None,
            last_shared_at: None,
            deleted_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
            field_versions: None,
        }
    }

    fn sample_file() -> FileRecord {
        FileRecord {
            id: "file-1".to_string(),
            folder_id: "folder-1".to_string(),
            sort_order: None,
            name: "notes.txt".to_string(),
            mime_type: "text/plain".to_string(),
            size: 42,
            data_url: None,
            checksum: "deadbeef".to_string(),
            version: 1,
            starred: false,
            last_cid: None,
            last_share_cid: None,
            deleted_at: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-02T00:00:00Z".to_string(),
            field_versions: None,
        }
    }

    #[test]
    fn file_bundle_serializes_camel_case_and_respects_omitempty() {
        let bundle = FileBundle {
            version: 1,
            exported_at: "2026-01-03T00:00:00Z".to_string(),
            origin_node: "node-1".to_string(),
            folder: sample_folder(),
            file: sample_file(),
        };

        let json = serde_json::to_string(&bundle).expect("serialize");

        // camelCase field names from both FileRecord and FolderRecord.
        assert!(json.contains("\"mimeType\""), "json: {json}");
        assert!(json.contains("\"folderId\""), "json: {json}");
        assert!(json.contains("\"sharedRoomId\""), "json: {json}");

        // `lastCid` is `omitempty` in Go: unset -> key omitted entirely.
        assert!(!json.contains("\"lastCid\""), "json: {json}");

        // `parentId` has no `omitempty` in Go: unset -> `null`, key present.
        assert!(json.contains("\"parentId\":null"), "json: {json}");
    }

    #[test]
    fn minimal_folder_json_defaults_optionals_to_none() {
        // Every field Go always emits is present -- including `parentId`,
        // which has no `omitempty` and so is `null` rather than absent.
        // Everything else (the true `omitempty` fields) is left out to
        // exercise the field-level `#[serde(default)]` leniency.
        let json = r##"{
            "id": "folder-1",
            "name": "My Folder",
            "parentId": null,
            "color": "#ff0000",
            "encrypted": false,
            "shareEnabled": false,
            "sharedRoomId": "",
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z"
        }"##;

        let folder: FolderRecord = serde_json::from_str(json).expect("deserialize");

        assert_eq!(folder.id, "folder-1");
        assert_eq!(folder.name, "My Folder");
        assert_eq!(folder.parent_id, None);
        assert_eq!(folder.sort_order, None);
        assert_eq!(folder.last_cid, None);
        assert_eq!(folder.last_saved_at, None);
        assert_eq!(folder.last_shared_at, None);
        assert_eq!(folder.deleted_at, None);
        assert!(folder.field_versions.is_none());
    }
}
