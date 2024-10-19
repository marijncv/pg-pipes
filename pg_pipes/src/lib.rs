use pgrx::{prelude::*, Json};
use serde::{de, Deserialize, Serialize};
use serde_json::json;
use std::fs::OpenOptions;
use std::io::Write;
use std::{collections::HashMap, fs::File};
::pgrx::pg_module_magic!();

#[derive(Debug, Deserialize)]
struct PipesContextData {
    asset_keys: Option<Vec<String>>,
    code_version_by_asset_key: Option<HashMap<String, Option<String>>>,
    partition_key: Option<String>,
    job_name: Option<String>,
    retry_number: i64,
    run_id: String,
    extras: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct PipesMessage {
    __dagster_pipes_version: String,
    method: String,
    params: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug)]
struct PipesTempFileMessageWriter {
    path: String,
}
impl PipesTempFileMessageWriter {
    fn write_message(&mut self, message: PipesMessage) {
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.path)
            .expect("could not open messages file");

        writeln!(file, "{}", serde_json::to_string(&message).unwrap())
            .expect("could not write to messages file");
    }
}

#[derive(Debug)]
struct PipesContext {
    data: PipesContextData,
    writer: PipesTempFileMessageWriter,
}
impl PipesContext {
    fn log(&mut self, message: &str, level: &str) {
        let params: HashMap<String, serde_json::Value> = HashMap::from([
            ("message".to_string(), json!(message)),
            ("level".to_string(), json!(level)),
        ]);
        let msg = PipesMessage {
            __dagster_pipes_version: "0.1".to_string(),
            method: "log".to_string(),
            params: Some(params),
        };
        self.writer.write_message(msg);
    }

    fn report_open(&mut self) {
        self.writer.write_message(PipesMessage {
            __dagster_pipes_version: "0.1".to_string(),
            method: "opened".to_string(),
            params: Some(HashMap::from([("extras".to_string(), json!("{}"))])),
        });
    }

    fn report_close(&mut self) {
        self.writer.write_message(PipesMessage {
            __dagster_pipes_version: "0.1".to_string(),
            method: "closed".to_string(),
            params: None,
        });
    }

    fn report_asset_materialization(&mut self, metadata: serde_json::Value) {
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
            __dagster_pipes_version: "0.1".to_string(),
            method: "report_asset_materialization".to_string(),
            params: Some(params),
        };
        self.writer.write_message(msg);
    }
}

fn open_dagster_pipes(context_path: &str, messages_path: &str) -> PipesContext {
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

#[pg_extern]
fn pipes_session(context_path: &str, messages_path: &str, query: &str) -> Result<(), spi::Error> {
    let mut context = open_dagster_pipes(context_path, messages_path);

    context.log("Started pipes session", "info");

    let analysis = Spi::connect(|mut client| {
        client
            .update(
                &format!("EXPLAIN (analyze 1, buffers 1, wal 1, format json) {query}"),
                None,
                None,
            )?
            .first()
            .get_one::<Json>()
    })?
    .unwrap();

    context.report_asset_materialization(
        json!({"analysis": {"raw_value": analysis.0, "type": "json"}})
    );

    context.report_close();

    Ok(())
}

