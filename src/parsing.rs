use std::collections::HashMap;
use csv::Reader;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub tx_type: TxType,
    pub client: u16,
    pub tx: u16,
    pub amount: Decimal,
}

#[derive(Debug, Serialize, Clone)]
pub struct Account {
    pub client: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

pub fn read_transactions_file(transactions_csv_path: &str) -> Result<Reader<File>, Box<dyn Error>> {
    let file = File::open(transactions_csv_path)?;

    let csv_reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(file);

    Ok(csv_reader)
}

pub fn write_accounts_file(client_accounts: HashMap<u16, Account>) -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(io::stdout());

    for account in client_accounts.values() {
        wtr.serialize(account)?;
    }
    
    wtr.flush()?;
    
    Ok(())
}