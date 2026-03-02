use crate::parsing::{Account, CsvTransaction};
use crate::transaction::{Transaction, TransactionWithAmount};
use csv::Reader;
use log::{debug, error, warn};
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;

enum StorageTransaction {
    Deposit(TransactionWithAmount),
    DisputeInProgress(TransactionWithAmount),
}

pub struct Processor {
    csv_reader: Reader<File>,
    client_accounts: BTreeMap<u16, Account>,
}

impl Processor {
    pub fn new(csv_reader: Reader<File>) -> Self {
        Processor {
            csv_reader,
            client_accounts: BTreeMap::new(),
        }
    }

    pub fn process(mut self) -> BTreeMap<u16, Account> {
        let mut txs_storage = HashMap::new();

        for (index, result) in self.csv_reader.deserialize().enumerate() {
            let csv_transaction: CsvTransaction = match result {
                Ok(r) => r,
                Err(e) => {
                    error!("Invalid line {}: {:?}", index + 2, e);
                    continue;
                }
            };

            let record: Transaction = match csv_transaction.try_into() {
                Ok(t) => t,
                Err(e) => {
                    error!("Invalid transaction on line {}: {}", index + 2, e);
                    continue;
                }
            };

            let client = record.client();
            let account = self.client_accounts.entry(client).or_insert_with(|| {
                let account = Account {
                    client,
                    available: Decimal::zero(),
                    held: Decimal::zero(),
                    total: Decimal::zero(),
                    locked: false,
                };

                debug!("Found new client {} account {:?}", account.client, account);

                account
            });

            Self::process_transaction(&mut txs_storage, index, record, account);
        }

        self.client_accounts
    }

    fn process_transaction(
        txs_storage: &mut HashMap<u32, StorageTransaction>,
        index: usize,
        record: Transaction,
        account: &mut Account,
    ) {
        match &record {
            Transaction::Deposit(tx) => {
                txs_storage.insert(tx.tx, StorageTransaction::Deposit(tx.clone()));

                account.available += tx.amount;
                account.total += tx.amount;
            }
            Transaction::Withdrawal(t) => {
                // TODO: Do withdrawals need to be disputed?

                account.available -= t.amount;
                account.total -= t.amount;
            }
            Transaction::Dispute(t) => {
                let storage_tx = if let Some(storage_tx) = txs_storage.get(&t.tx) {
                    storage_tx
                } else {
                    warn!(
                        "Line {}, disputed transaction {} for client {} doesn't exist",
                        index + 2,
                        t.tx,
                        t.client,
                    );

                    return;
                };

                match storage_tx {
                    StorageTransaction::Deposit(tx) => {
                        account.available -= tx.amount;
                        account.held += tx.amount;

                        txs_storage.insert(t.tx, StorageTransaction::DisputeInProgress(tx.clone()));
                    }
                    _ => {
                        warn!(
                            "Line {}, transaction {} for client {} is not a deposit and can't be disputed",
                            index + 2, t.tx, t.client,
                        );
                    }
                }
            }
            Transaction::Resolve(t) => {
                let initial_tx = if let Some(initial_tx) = txs_storage.get(&t.tx) {
                    initial_tx
                } else {
                    warn!(
                        "Line {}, resolved transaction {} for client {} doesn't exist",
                        index + 2,
                        t.tx,
                        t.client,
                    );

                    return;
                };

                match initial_tx {
                    StorageTransaction::DisputeInProgress(tx) => {
                        account.available += tx.amount;
                        account.held -= tx.amount;

                        let tx_id = tx.tx;

                        txs_storage.remove(&tx_id);
                    }
                    _ => {
                        warn!(
                            "Line {}, transaction {} for client {} is not a dispute and can't be resolved",
                            index + 2, t.tx, t.client,
                        );
                    }
                }
            }
            Transaction::Chargeback(t) => {
                let initial_tx = if let Some(initial_tx) = txs_storage.get(&t.tx) {
                    initial_tx
                } else {
                    warn!(
                        "Line {}, chargeback transaction {} for client {} doesn't exist",
                        index + 2,
                        t.tx,
                        t.client,
                    );

                    return;
                };

                match initial_tx {
                    StorageTransaction::DisputeInProgress(tx) => {
                        account.held -= tx.amount;
                        account.total -= tx.amount;
                        account.locked = true;

                        let tx_id = tx.tx;

                        txs_storage.remove(&tx_id);
                    }
                    _ => {
                        warn!(
                            "Line {}, transaction {} for client {} is not a dispute and can't be charged back",
                            index + 2, t.tx, t.client,
                        );
                        return;
                    }
                }
            }
        }

        debug!("Modified client {} account {:?}", account.client, account);
    }
}
