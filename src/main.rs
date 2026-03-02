use crate::parsing::{read_transactions_file, write_accounts_file};
use crate::processor::Processor;
use clap::Parser;
use log::{error, info};

mod parsing;
mod processor;
mod transaction;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg()]
    transactions_csv_path: String,
}

fn main() {
    env_logger::init();

    let args = Args::parse();

    let csv_reader = match read_transactions_file(&args.transactions_csv_path) {
        Ok(csv_reader) => csv_reader,
        Err(e) => {
            error!("Error while reading transactions file: {:?}", e);
            return;
        }
    };

    let processing = Processor::new(csv_reader);

    let client_accounts = processing.process();

    let result = write_accounts_file(client_accounts);

    match result {
        Ok(_) => {
            info!("Successfully written accounts!");
        }
        Err(e) => {
            error!("Error while writing accounts: {:?}", e);
        }
    }
}
