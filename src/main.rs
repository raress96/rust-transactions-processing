use crate::parsing::{read_transactions_file, write_accounts_file, Account, Transaction, TxType};
use clap::Parser;
use log::{debug, error, info};
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use std::collections::HashMap;

mod parsing;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg()]
    transactions_csv_path: String,
}

fn main() {
    env_logger::init();

    let args = Args::parse();

    debug!("Input: {}!", args.transactions_csv_path);

    let mut csv_reader = match read_transactions_file(&args.transactions_csv_path) {
        Ok(csv_reader) => csv_reader,
        Err(e) => {
            error!("Error while reading file: {:?}", e);
            return;
        }
    };

    // TODO: Improve performance
    let mut client_accounts = HashMap::new();

    for (index, result) in csv_reader.deserialize().enumerate() {
        let record: Transaction = match result {
            Ok(r) => r,
            Err(e) => {
                error!("Skipping line {}: {:?}", index + 2, e);
                continue;
            }
        };

        // TODO: Handle proper amounts
        client_accounts
            .entry(record.client)
            .and_modify(|account: &mut Account| {
                let old_account = account.clone();

                match record.tx_type {
                    TxType::Deposit => {
                        account.available += record.amount;
                        account.total += record.amount;
                    }
                    TxType::Withdrawal => {
                        account.available -= record.amount;
                        account.total -= record.amount;
                    }
                    // TODO: Implement other cases
                    TxType::Dispute => {}
                    TxType::Resolve => {}
                    TxType::Chargeback => {}
                }

                debug!(
                    "Modified client {} account from {:?} to {:?}",
                    account.client, old_account, account
                );
            })
            .or_insert_with(|| {
                let (available, total) = match record.tx_type {
                    TxType::Deposit => (record.amount, record.amount),
                    TxType::Withdrawal => (-record.amount, -record.amount),
                    _ => (Decimal::zero(), Decimal::zero()),
                };

                let account = Account {
                    client: record.client,
                    available,
                    held: Decimal::zero(),
                    total,
                    locked: false,
                };

                debug!("Found new client {} account {:?}", account.client, account);

                account
            });
    }

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
