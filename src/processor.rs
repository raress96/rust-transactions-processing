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
            let line = index + 2;

            let csv_transaction: CsvTransaction = match result {
                Ok(r) => r,
                Err(e) => {
                    error!("Invalid line {}: {}", line, e);
                    continue;
                }
            };

            let record: Transaction = match csv_transaction.try_into() {
                Ok(t) => t,
                Err(e) => {
                    error!("Invalid transaction on line {}: {}", line, e);
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

            Self::process_transaction(&mut txs_storage, line, record, account);
        }

        self.client_accounts
    }

    fn process_transaction(
        txs_storage: &mut HashMap<u32, StorageTransaction>,
        line: usize,
        record: Transaction,
        account: &mut Account,
    ) {
        if account.locked {
            warn!(
                "Line {}, account {} is locked, transaction {:?} not accepted",
                line, account.client, record
            );
            return;
        }

        match &record {
            Transaction::Deposit(t) => {
                txs_storage.insert(t.tx, StorageTransaction::Deposit(t.clone()));

                account.available += t.amount;
                account.total += t.amount;
            }
            Transaction::Withdrawal(t) => {
                if t.amount > account.available {
                    warn!(
                        "Line {}, account {} doesn't have enough funds for withdrawal transaction {:?}",
                        line, account.client, t
                    );
                    return;
                }

                account.available -= t.amount;
                account.total -= t.amount;
            }
            Transaction::Dispute(t) => {
                let storage_tx = if let Some(storage_tx) = txs_storage.get(&t.tx) {
                    storage_tx
                } else {
                    warn!(
                        "Line {}, disputed transaction {} for client {} doesn't exist",
                        line, t.tx, t.client,
                    );

                    return;
                };

                match storage_tx {
                    StorageTransaction::Deposit(initial_tx) => {
                        if initial_tx.client != t.client {
                            warn!(
                                "Line {}, disputed transaction {} is for a different client {} than original deposit client {}",
                                line, t.tx, t.client, initial_tx.client
                            );
                            return;
                        }

                        account.available -= initial_tx.amount;
                        account.held += initial_tx.amount;

                        txs_storage.insert(
                            t.tx,
                            StorageTransaction::DisputeInProgress(initial_tx.clone()),
                        );
                    }
                    _ => {
                        warn!(
                            "Line {}, transaction {} for client {} is not a deposit and can't be disputed",
                            line, t.tx, t.client,
                        );
                    }
                }
            }
            Transaction::Resolve(t) => {
                let storage_tx = if let Some(storage_tx) = txs_storage.get(&t.tx) {
                    storage_tx
                } else {
                    warn!(
                        "Line {}, resolved transaction {} for client {} doesn't exist",
                        line, t.tx, t.client,
                    );

                    return;
                };

                match storage_tx {
                    StorageTransaction::DisputeInProgress(initial_tx) => {
                        if initial_tx.client != t.client {
                            warn!(
                                "Line {}, disputed transaction {} is for a different client {} than original deposit client {}",
                                line, t.tx, t.client, initial_tx.client
                            );
                            return;
                        }

                        account.available += initial_tx.amount;
                        account.held -= initial_tx.amount;

                        // Insert transaction back so it can possibly be disputed again
                        txs_storage.insert(t.tx, StorageTransaction::Deposit(initial_tx.clone()));
                    }
                    _ => {
                        warn!(
                            "Line {}, transaction {} for client {} is not a dispute and can't be resolved",
                            line, t.tx, t.client,
                        );
                    }
                }
            }
            Transaction::Chargeback(t) => {
                let storage_tx = if let Some(storage_tx) = txs_storage.get(&t.tx) {
                    storage_tx
                } else {
                    warn!(
                        "Line {}, chargeback transaction {} for client {} doesn't exist",
                        line, t.tx, t.client,
                    );

                    return;
                };

                match storage_tx {
                    StorageTransaction::DisputeInProgress(initial_tx) => {
                        if initial_tx.client != t.client {
                            warn!(
                                "Line {}, disputed transaction {} is for a different client {} than original deposit client {}",
                                line, t.tx, t.client, initial_tx.client
                            );
                            return;
                        }

                        account.held -= initial_tx.amount;
                        account.total -= initial_tx.amount;
                        account.locked = true;

                        let tx_id = initial_tx.tx;

                        // Account is already locked so this transaction can no longer be disputed again
                        // We can remove it for cleaning up memory usage
                        txs_storage.remove(&tx_id);
                    }
                    _ => {
                        warn!(
                            "Line {}, transaction {} for client {} is not a dispute and can't be charged back",
                            line, t.tx, t.client,
                        );
                    }
                }
            }
        }

        debug!("Modified client {} account {:?}", account.client, account);
    }
}
