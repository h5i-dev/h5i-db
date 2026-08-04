//! Cell outputs and mime bundles (nbformat v4 `output` objects).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// A mime bundle: mime type to payload.
///
/// Text payloads are stored joined (a single `String` value) even though the
/// on-disk form splits them into line arrays. The split/join happens once at
/// the JSON boundary in [`crate::document::json`], the way nbformat itself
/// does it, so nothing above this type has to think about it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MimeBundle(pub Map<String, Value>);

impl MimeBundle {
    pub fn new() -> Self {
        Self(Map::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, mime: &str) -> Option<&Value> {
        self.0.get(mime)
    }

    pub fn insert(&mut self, mime: impl Into<String>, value: Value) {
        self.0.insert(mime.into(), value);
    }

    pub fn mime_types(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// `text/plain`, the representation every kernel is required to provide.
    pub fn text_plain(&self) -> Option<&str> {
        self.0.get("text/plain").and_then(Value::as_str)
    }

    /// First mime type in `priority` that this bundle carries.
    ///
    /// Callers pass their own priority list because the answer differs by
    /// audience: a TUI wants the image, an agent digest wants the text.
    pub fn richest<'a>(&self, priority: &[&'a str]) -> Option<&'a str> {
        priority.iter().copied().find(|m| self.0.contains_key(*m))
    }

    /// Payload as a string, for the text-ish mime types.
    pub fn text_of(&self, mime: &str) -> Option<&str> {
        self.0.get(mime).and_then(Value::as_str)
    }
}

/// Which standard stream a `stream` output came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamName {
    Stdout,
    Stderr,
}

impl StreamName {
    pub fn as_str(self) -> &'static str {
        match self {
            StreamName::Stdout => "stdout",
            StreamName::Stderr => "stderr",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamOutput {
    pub name: StreamName,
    #[serde(default)]
    pub text: String,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisplayDataOutput {
    #[serde(default)]
    pub data: MimeBundle,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteResultOutput {
    #[serde(default)]
    pub data: MimeBundle,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    /// Serialized even when `None` (as JSON `null`): nbformat requires the key
    /// to be present on `execute_result`.
    pub execution_count: Option<i64>,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorOutput {
    #[serde(default)]
    pub ename: String,
    #[serde(default)]
    pub evalue: String,
    #[serde(default)]
    pub traceback: Vec<String>,
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extra: Map<String, Value>,
}

/// One entry of a code cell's `outputs` array.
///
/// `Unknown` exists so that a notebook carrying an output type this build does
/// not model survives a load/save cycle byte for byte instead of being dropped
/// or rejected. Silently discarding a user's data is the worse failure.
#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Stream(StreamOutput),
    DisplayData(DisplayDataOutput),
    ExecuteResult(ExecuteResultOutput),
    Error(ErrorOutput),
    Unknown(Map<String, Value>),
}

impl Output {
    pub fn output_type(&self) -> &str {
        match self {
            Output::Stream(_) => "stream",
            Output::DisplayData(_) => "display_data",
            Output::ExecuteResult(_) => "execute_result",
            Output::Error(_) => "error",
            Output::Unknown(map) => map
                .get("output_type")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        }
    }

    /// The mime bundle, for the output types that carry one.
    pub fn data(&self) -> Option<&MimeBundle> {
        match self {
            Output::DisplayData(o) => Some(&o.data),
            Output::ExecuteResult(o) => Some(&o.data),
            _ => None,
        }
    }

    pub fn stream(name: StreamName, text: impl Into<String>) -> Self {
        Output::Stream(StreamOutput {
            name,
            text: text.into(),
            extra: Map::new(),
        })
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Output::Error(_))
    }

    /// `display_id` from the output metadata, used to match
    /// `update_display_data` against an already-emitted output.
    pub fn display_id(&self) -> Option<&str> {
        let meta = match self {
            Output::DisplayData(o) => &o.metadata,
            Output::ExecuteResult(o) => &o.metadata,
            _ => return None,
        };
        meta.get("display_id").and_then(Value::as_str)
    }

    fn from_map(mut map: Map<String, Value>) -> Result<Self> {
        let Some(tag) = map
            .get("output_type")
            .and_then(Value::as_str)
            .map(str::to_owned)
        else {
            return Ok(Output::Unknown(map));
        };
        map.remove("output_type");
        let rest = Value::Object(map);
        // A payload that does not fit its declared shape is kept verbatim
        // rather than rejected: we are a notebook tool, not a validator, and
        // refusing to open a slightly-off file helps nobody.
        let recover = |v: Value| match v {
            Value::Object(mut m) => {
                m.insert("output_type".into(), Value::String(tag.clone()));
                Output::Unknown(m)
            }
            other => Output::Unknown(
                [
                    ("output_type".to_string(), Value::String(tag.clone())),
                    ("value".to_string(), other),
                ]
                .into_iter()
                .collect(),
            ),
        };
        Ok(match tag.as_str() {
            "stream" => serde_json::from_value(rest.clone())
                .map(Output::Stream)
                .unwrap_or_else(|_| recover(rest)),
            "display_data" => serde_json::from_value(rest.clone())
                .map(Output::DisplayData)
                .unwrap_or_else(|_| recover(rest)),
            "execute_result" => serde_json::from_value(rest.clone())
                .map(Output::ExecuteResult)
                .unwrap_or_else(|_| recover(rest)),
            "error" => serde_json::from_value(rest.clone())
                .map(Output::Error)
                .unwrap_or_else(|_| recover(rest)),
            _ => recover(rest),
        })
    }

    fn to_map(&self) -> Result<Map<String, Value>> {
        if let Output::Unknown(map) = self {
            return Ok(map.clone());
        }
        let mut map = match serde_json::to_value(SerdeBody(self)).map_err(Error::internal)? {
            Value::Object(m) => m,
            other => {
                return Err(Error::internal(format!(
                    "output serialized to a non-object: {other}"
                )));
            }
        };
        map.insert(
            "output_type".into(),
            Value::String(self.output_type().to_string()),
        );
        Ok(map)
    }
}

/// Serializes only the payload; the `output_type` tag is added by `to_map`.
struct SerdeBody<'a>(&'a Output);

impl Serialize for SerdeBody<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            Output::Stream(o) => o.serialize(s),
            Output::DisplayData(o) => o.serialize(s),
            Output::ExecuteResult(o) => o.serialize(s),
            Output::Error(o) => o.serialize(s),
            Output::Unknown(m) => m.serialize(s),
        }
    }
}

impl Serialize for Output {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.to_map()
            .map_err(serde::ser::Error::custom)?
            .serialize(s)
    }
}

impl<'de> Deserialize<'de> for Output {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let map = Map::deserialize(d)?;
        Output::from_map(map).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stream_round_trips() {
        let v = json!({"output_type": "stream", "name": "stdout", "text": "hi\n"});
        let o: Output = serde_json::from_value(v.clone()).unwrap();
        assert!(matches!(&o, Output::Stream(s) if s.name == StreamName::Stdout));
        assert_eq!(serde_json::to_value(&o).unwrap(), v);
    }

    #[test]
    fn execute_result_keeps_a_null_execution_count_key() {
        // nbformat requires the key to exist; dropping it makes the notebook
        // fail validation in Jupyter.
        let v = json!({
            "output_type": "execute_result",
            "data": {"text/plain": "42"},
            "metadata": {},
            "execution_count": null
        });
        let o: Output = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(serde_json::to_value(&o).unwrap(), v);
    }

    #[test]
    fn unknown_output_types_survive_untouched() {
        let v = json!({"output_type": "future_thing", "payload": [1, 2, 3]});
        let o: Output = serde_json::from_value(v.clone()).unwrap();
        assert!(matches!(o, Output::Unknown(_)));
        assert_eq!(serde_json::to_value(&o).unwrap(), v);
    }

    #[test]
    fn a_malformed_known_output_is_preserved_rather_than_dropped() {
        // `traceback` as a string instead of a list: real files in the wild do
        // this. We keep it verbatim instead of erroring the whole notebook.
        let v = json!({"output_type": "error", "ename": "E", "evalue": "v", "traceback": "flat"});
        let o: Output = serde_json::from_value(v.clone()).unwrap();
        assert!(matches!(o, Output::Unknown(_)));
        assert_eq!(serde_json::to_value(&o).unwrap(), v);
    }

    #[test]
    fn unmodelled_keys_on_a_known_output_survive() {
        let v = json!({
            "output_type": "display_data",
            "data": {"text/plain": "x"},
            "metadata": {},
            "transient": {"display_id": "abc"}
        });
        let o: Output = serde_json::from_value(v.clone()).unwrap();
        assert!(matches!(o, Output::DisplayData(_)));
        assert_eq!(serde_json::to_value(&o).unwrap(), v);
    }

    #[test]
    fn richest_picks_by_caller_priority() {
        let mut b = MimeBundle::new();
        b.insert("text/plain", json!("x"));
        b.insert("image/png", json!("iVBOR"));
        assert_eq!(b.richest(&["image/png", "text/plain"]), Some("image/png"));
        assert_eq!(b.richest(&["text/html", "text/plain"]), Some("text/plain"));
        assert_eq!(b.richest(&["text/html"]), None);
    }
}
