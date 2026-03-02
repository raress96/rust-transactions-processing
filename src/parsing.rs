use csv::Reader;
use log::debug;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io;

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CsvTransaction {
    #[serde(rename = "type")]
    pub tx_type: TxType,
    pub client: u16,
    pub tx: u32,
    pub amount: Option<Decimal>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Account {
    pub client: u16,
    #[serde(serialize_with = "serialize_decimal")]
    pub available: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    pub held: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    pub total: Decimal,
    pub locked: bool,
}

pub fn read_transactions_file(transactions_csv_path: &str) -> Result<Reader<File>, Box<dyn Error>> {
    debug!("Reading transactions from: {}", transactions_csv_path);

    let file = File::open(transactions_csv_path)?;

    let csv_reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(file);

    Ok(csv_reader)
}

pub fn write_accounts_file(client_accounts: BTreeMap<u16, Account>) -> Result<(), Box<dyn Error>> {
    let mut wtr = csv::Writer::from_writer(io::stdout());

    for account in client_accounts.values() {
        wtr.serialize(account)?;
    }

    wtr.flush()?;

    Ok(())
}

fn serialize_decimal<S: Serializer>(value: &Decimal, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&value.round_dp(4).to_string())
}
