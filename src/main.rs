use crate::parsing::{read_transactions_file, write_accounts};
use crate::processor::Processor;
use clap::Parser;
use log::{error, info};
use std::process::ExitCode;

mod parsing;
mod processor;
mod transaction;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg()]
    transactions_csv_path: String,
}

fn main() -> ExitCode {
    env_logger::init();

    let args = Args::parse();

    let csv_reader = match read_transactions_file(&args.transactions_csv_path) {
        Ok(csv_reader) => csv_reader,
        Err(e) => {
            error!("Error while reading transactions file: {}", e);
            return ExitCode::from(1);
        }
    };

    let processor = Processor::new(csv_reader);

    let client_accounts = processor.process();

    let result = write_accounts(client_accounts);

    match result {
        Ok(_) => {
            info!("Successfully written accounts!");
        }
        Err(e) => {
            error!("Error while writing accounts: {}", e);
            return ExitCode::from(2);
        }
    }

    ExitCode::SUCCESS
}
