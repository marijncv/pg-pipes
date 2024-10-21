use pgrx::{prelude::*, Json};
use serde_json::json;
::pgrx::pg_module_magic!();
mod pipes;

use pipes::open_dagster_pipes;

extension_sql!(
    r#"
CREATE TABLE example (
    id serial8 not null primary key,
    value text
);
"#,
    name = "create_example_table",
);

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

    context.log("Analysis complete", "info");

    let analysis = analysis.0.get(0).unwrap();

    let plan = analysis.get("Plan").unwrap();
    let planning = analysis.get("Planning").unwrap();
    let planning_time = analysis.get("Planning Time").unwrap();
    let triggers = analysis.get("Triggers").unwrap();
    let execution_time = analysis.get("Execution Time").unwrap();

    context.report_asset_materialization(json!({
        "plan": {"raw_value": plan, "type": "json"},
        "planning": {"raw_value": planning, "type": "json"},
        "planning_time": {"raw_value": planning_time, "type": "float"},
        "triggers": {"raw_value": triggers, "type": "json"},
        "execution_time": {"raw_value": execution_time, "type": "float"}
    }));

    context.report_close();

    Ok(())
}
