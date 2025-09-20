//! Command-line interface exposing core DeltaClient functionality.

use clap::Parser;
use delta_operations::{DeltaClient, Result, WriteMode};
use std::io::{self, Write};
use datafusion::prelude::*; // for merge expressions
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(name = "delta-repl", about = "Interactive Delta Lake CLI", version)]
struct Cli {
    /// Optional one-shot command (e.g. "sql SELECT 1") executed before entering REPL if provided
    #[arg(last = true)]
    oneshot: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber (env RUST_LOG can override level)
    use tracing_subscriber::{fmt, EnvFilter};
    let _ = fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(true)
        .try_init();

    let cli = Cli::parse();
    let mut client = DeltaClient::new().await?;

    if !cli.oneshot.is_empty() {
        handle_command(&mut client, &cli.oneshot.join(" ")).await;
    }

    println!("Delta REPL (type 'help' for commands, 'exit' to quit)");
    let stdin = io::stdin();
    loop {
        print!(">> ");
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.read_line(&mut line).is_err() { continue; }
        let input = line.trim();
        if input.is_empty() { continue; }
        if matches!(input, "exit" | "quit" ) { break; }
        if input == "help" { print_help(); continue; }
        handle_command(&mut client, input).await;
    }
    Ok(())
}

fn print_help() {
    println!("Commands:");
    println!("  load <path> [alias]        Load a Delta table (random alias if omitted)");
    println!("  tables                     List loaded tables");
    println!("  read <alias> [limit]       Read SELECT * from alias (default limit 20)");
    println!("  sql <query>                Execute arbitrary SQL");
    println!("  schema <alias>             Show schema for a loaded table");
    println!("  cdf <path> <fromVersion>   Read change data feed starting at version");
    println!("  write <alias> <path> <mode>  Write table alias to path (mode: append|overwrite)");
    println!("  merge <source_alias> <target_path> <key_col?>  Merge source into target on key (default 'id')");
    println!("  optimize <path>            Optimize (compact) a Delta table");
    println!("  vacuum <path>              Vacuum a Delta table (remove old unreferenced files)");
    println!("  enable_cdf <path>          Attempt to enable change data feed (limited; may return warning)");
    println!("  hard_delete <path>         Physically remove all table files (DANGEROUS)");
    println!("  unload <alias>             Deregister alias");
    println!("  help                       Show this help");
    println!("  exit                       Quit");
}

async fn handle_command(client: &mut DeltaClient, input: &str) {
    let mut parts = input.split_whitespace();
    let cmd = match parts.next() { Some(c) => c, None => return };
    let rest = input[cmd.len()..].trim();
    let outcome = match cmd {
        "load" => {
            if rest.is_empty() { return warn("Usage: load <path> [alias]"); }
            let mut p = rest.split_whitespace();
            let path = p.next().unwrap();
            if let Some(alias) = p.next() {
                match client.load_table_with_alias(path, alias).await {
                    Ok(t) => info(&format!("Loaded {} as '{}'(v{})", path, alias, t.version().unwrap_or(0))),
                    Err(e) => err(&e.to_string()),
                }
            } else {
                match client.load_table(path).await {
                    Ok((_t,a)) => info(&format!("Loaded {} as '{}'", path, a)),
                    Err(e) => err(&e.to_string()),
                }
            }
        }
        "tables" => {
            let rows = client.list_tables();
            if rows.is_empty() { info("No tables loaded"); }
            else { for (a,u) in rows { println!("{:10} -> {}", a, u); } }
        }
        "read" => {
            if rest.is_empty() { return warn("Usage: read <alias> [limit]"); }
            let mut p = rest.split_whitespace();
            let alias = p.next().unwrap();
            let limit: usize = p.next().and_then(|s| s.parse().ok()).unwrap_or(20);
            match client.session_context.table(alias).await {
                Ok(df) => match df.limit(0, Some(limit)).and_then(|df| Ok(df)) {
                    Ok(df) => match df.collect().await { Ok(batches) => { for b in batches { println!("{:?}", b); } }, Err(e) => err(&e.to_string())},
                    Err(e) => err(&e.to_string())
                },
                Err(e) => err(&e.to_string())
            }
        }
        "sql" => {
            if rest.is_empty() { return warn("Usage: sql <query>"); }
            match client.sql(rest).await {
                Ok(df) => match df.collect().await { Ok(batches) => { for b in batches { println!("{:?}", b); } }, Err(e) => err(&e.to_string())},
                Err(e) => err(&e.to_string())
            }
        }
        "schema" => {
            if rest.is_empty() { return warn("Usage: schema <alias>"); }
            match client.session_context.table(rest).await {
                Ok(df) => {
                    let schema = df.schema();
                    println!("Schema for '{}':", rest);
                    for field in schema.fields() { println!("  {} ({:?})", field.name(), field.data_type()); }
                }
                Err(e) => err(&e.to_string())
            }
        }
        "cdf" => {
            let mut p = rest.split_whitespace();
            let path = match p.next() { Some(v) => v, None => return warn("Usage: cdf <path> <fromVersion>") };
            let from: i64 = p.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            match client.read_cdf(path, from).await {
                Ok(df) => match df.collect().await { Ok(batches) => { for b in batches { println!("{:?}", b); } }, Err(e) => err(&e.to_string())},
                Err(e) => err(&e.to_string())
            }
        }
        "write" => {
            // write <alias> <path> <mode>
            let mut p = rest.split_whitespace();
            let alias = match p.next() { Some(a) => a, None => return warn("Usage: write <alias> <path> <mode>") };
            let path = match p.next() { Some(p2) => p2, None => return warn("Usage: write <alias> <path> <mode>") };
            let mode_str = p.next().unwrap_or("append").to_lowercase();
            let mode = match mode_str.as_str() { "append" => WriteMode::Append, "overwrite" => WriteMode::Overwrite, other => { return warn(&format!("Unsupported mode: {}", other)); } };
            match client.session_context.table(alias).await {
                Ok(df) => match client.write(df, path, mode).await { Ok(t) => info(&format!("Wrote alias '{}' to {} (v{})", alias, path, t.version().unwrap_or(0))), Err(e) => err(&e.to_string())},
                Err(e) => err(&e.to_string())
            }
        }
        "unload" => {
            if rest.is_empty() { return warn("Usage: unload <alias>"); }
            match client.unload_table(rest) { Ok(_) => info(&format!("Unloaded {}", rest)), Err(e) => err(&e.to_string()) }
        }
        "merge" => {
            // merge <source_alias> <target_path> <key_col?>
            let mut p = rest.split_whitespace();
            let source_alias = match p.next() { Some(a) => a, None => return warn("Usage: merge <source_alias> <target_path> <key_col?>") };
            let target_path = match p.next() { Some(p2) => p2, None => return warn("Usage: merge <source_alias> <target_path> <key_col?>") };
            let key_col = p.next().unwrap_or("id");
            match client.session_context.table(source_alias).await {
                Ok(source_df) => {
                    // Capture field names early to avoid borrow issues
                    let field_names: Vec<String> = source_df.schema().fields().iter().map(|f| f.name().to_string()).collect();
                    let merge_condition = col(&format!("t.{}", key_col)).eq(col(&format!("s.{}", key_col)));
                    let match_condition = lit(true);
                    let mut update_map: HashMap<&str, Expr> = HashMap::new();
                    let mut insert_map: HashMap<&str, Expr> = HashMap::new();
                    for name in &field_names {
                        update_map.insert(name.as_str(), col(&format!("s.{}", name)));
                        insert_map.insert(name.as_str(), col(&format!("s.{}", name)));
                    }
                    match client.merge(
                        source_df,
                        target_path,
                        merge_condition,
                        match_condition,
                        Some(update_map),
                        Some(insert_map),
                        true,
                        false
                    ).await {
                        Ok(t) => info(&format!("Merge complete (table v{})", t.version().unwrap_or(0))),
                        Err(e) => err(&e.to_string())
                    }
                }
                Err(e) => err(&e.to_string())
            }
        }
        "optimize" => {
            if rest.is_empty() { return warn("Usage: optimize <path>"); }
            match client.optimize(rest).await { Ok(t) => info(&format!("Optimize complete (table v{})", t.version().unwrap_or(0))), Err(e) => err(&e.to_string()) }
        }
        "vacuum" => {
            if rest.is_empty() { return warn("Usage: vacuum <path>"); }
            match client.vacuum(rest).await { Ok(t) => info(&format!("Vacuum complete (table v{})", t.version().unwrap_or(0))), Err(e) => err(&e.to_string()) }
        }
        "enable_cdf" => {
            if rest.is_empty() { return warn("Usage: enable_cdf <path>"); }
            match client.enable_change_data_feed(rest).await { Ok(_) => info("CDF already enabled or now active"), Err(e) => warn(&format!("CDF enable attempt: {}", e)) }
        }
        "hard_delete" => {
            if rest.is_empty() { return warn("Usage: hard_delete <path>"); }
            match client.hard_delete_table(rest).await { Ok(_) => info("Hard delete workflow completed"), Err(e) => err(&e.to_string()) }
        }
        _ => warn("Unknown command (type 'help')"),
    };
    let _ = outcome; // we log inline; this keeps signature consistent
}

fn info(msg: &str) { println!("✅ {}", msg); }
fn warn(msg: &str) { println!("⚠️  {}", msg); }
fn err(msg: &str) { eprintln!("❌ {}", msg); }
