use derivative::Derivative;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::OpenOptions;
use std::io::Write;
use std::{collections::HashMap, fs::File};

enum Method {
    Opened,
    Closed,
    Log,
    ReportAssetMaterialization,
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Self::Opened => write!(f, "opened"),
            Self::Closed => write!(f, "closed"),
            Self::Log => write!(f, "log"),
            Self::ReportAssetMaterialization => write!(f, "report_asset_materialization"),
        }
    }
}

#[derive(Deserialize)]
struct PipesContextData {
    asset_keys: Option<Vec<String>>,
}

#[derive(Derivative)]
#[derivative(Default)]
#[derive(Serialize)]
struct PipesMessage {
    #[derivative(Default(value = "\"0.1\".to_string()"))]
    __dagster_pipes_version: String,
    method: String,
    params: Option<HashMap<String, serde_json::Value>>,
}

struct PipesTempFileMessageWriter {
    path: String,
}
impl PipesTempFileMessageWriter {
    fn write_message(&mut self, message: PipesMessage) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .expect("could not open messages file");

        writeln!(file, "{}", serde_json::to_string(&message).unwrap())
            .expect("could not write to messages file");
    }
}

pub struct PipesContext {
    data: PipesContextData,
    writer: PipesTempFileMessageWriter,
}
impl PipesContext {
    pub fn log(&mut self, message: &str, level: &str) {
        let params: HashMap<String, serde_json::Value> = HashMap::from([
            ("message".to_string(), json!(message)),
            ("level".to_string(), json!(level)),
        ]);
        let msg = PipesMessage {
            method: Method::Log.to_string(),
            params: Some(params),
            ..Default::default()
        };
        self.writer.write_message(msg);
    }

    pub fn report_open(&mut self) {
        self.writer.write_message(PipesMessage {
            method: Method::Opened.to_string(),
            params: Some(HashMap::from([("extras".to_string(), json!("{}"))])),
            ..Default::default()
        });
    }

    pub fn report_close(&mut self) {
        self.writer.write_message(PipesMessage {
            method: Method::Closed.to_string(),
            params: None,
            ..Default::default()
        });
    }

    pub fn report_asset_materialization(&mut self, metadata: serde_json::Value) {
        let asset_key = self
            .data
            .asset_keys
            .clone()
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let params: HashMap<String, serde_json::Value> = HashMap::from([
            ("asset_key".to_string(), json!(asset_key)),
            ("metadata".to_string(), metadata),
            ("data_version".to_string(), serde_json::Value::Null),
        ]);
        let msg = PipesMessage {
            method: Method::ReportAssetMaterialization.to_string(),
            params: Some(params),
            ..Default::default()
        };
        self.writer.write_message(msg);
    }
}

pub fn open_dagster_pipes(context_path: &str, messages_path: &str) -> PipesContext {
    let context_file = File::open(context_path).expect("could not open context file");
    let context_data: PipesContextData =
        serde_json::from_reader(context_file).expect("could not serialize from context file");

    let mut pipes_context = PipesContext {
        data: context_data,
        writer: PipesTempFileMessageWriter {
            path: messages_path.to_string(),
        },
    };

    pipes_context.report_open();

    pipes_context
}
