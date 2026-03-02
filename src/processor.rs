use crate::parsing::{Account, CsvTransaction};
use crate::transaction::{Transaction, TransactionWithAmount};
use csv::Reader;
use log::{debug, error, warn};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, PartialEq)]
enum StorageTransaction {
    Deposit(TransactionWithAmount),
    DisputeInProgress(TransactionWithAmount),
}

pub struct Processor<R> {
    csv_reader: Reader<R>,
}

impl<R> Processor<R>
where
    R: std::io::Read,
{
    pub fn new(csv_reader: Reader<R>) -> Self {
        Processor { csv_reader }
    }

    pub fn process(mut self) -> BTreeMap<u16, Account> {
        let mut client_accounts = BTreeMap::new();
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
            let account = client_accounts.entry(client).or_insert_with(|| {
                debug!("Found new client {}", client);

                Account::new(client)
            });

            process_transaction(&mut txs_storage, line, record, account);
        }

        client_accounts
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::transaction::TransactionWithoutAmount;
    use rust_decimal::prelude::{FromPrimitive, Zero};
    use rust_decimal::Decimal;

    #[test]
    fn test_process_transaction_locked() {
        let mut account = Account::new(1);
        account.locked = true;

        process_transaction(
            &mut HashMap::new(),
            1,
            Transaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
            &mut account,
        );

        assert_eq!(account.total, Decimal::zero());
    }

    #[test]
    fn test_process_transaction_deposit() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
            &mut account,
        );

        assert_eq!(
            account,
            Account {
                client: 1,
                total: decimal_one(),
                available: decimal_one(),
                held: Decimal::zero(),
                locked: false
            }
        );

        assert!(txs_storage.get(&1).is_some());
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_withdrawal_err() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Withdrawal(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
    }

    #[test]
    fn test_process_transaction_withdrawal() {
        let mut account = Account::new(1);
        account.available = decimal_one();
        account.total = decimal_one();
        let mut txs_storage = HashMap::new();

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Withdrawal(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
            &mut account,
        );

        assert_eq!(
            account,
            Account {
                client: 1,
                total: Decimal::zero(),
                available: Decimal::zero(),
                held: Decimal::zero(),
                locked: false
            }
        );
    }

    #[test]
    fn test_process_transaction_dispute_non_existent() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Dispute(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
    }

    #[test]
    fn test_process_transaction_dispute_other_client() {
        let mut account = Account::new(2);
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Dispute(TransactionWithoutAmount { client: 2, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(2));
        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_dispute_already_disputed() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Dispute(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_dispute() {
        let mut account = Account::new(1);
        account.available = decimal_one();
        account.total = decimal_one();
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Dispute(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(
            account,
            Account {
                client: 1,
                available: Decimal::zero(),
                held: decimal_one(),
                total: decimal_one(),
                locked: false
            }
        );

        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_resolve_non_existent() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Resolve(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
    }

    #[test]
    fn test_process_transaction_resolve_other_client() {
        let mut account = Account::new(2);
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Resolve(TransactionWithoutAmount { client: 2, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(2));
        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_resolve_not_disputed() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Resolve(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_resolve() {
        let mut account = Account::new(1);
        account.available = Decimal::zero();
        account.held = decimal_one();
        account.total = decimal_one();
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Resolve(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(
            account,
            Account {
                client: 1,
                available: decimal_one(),
                held: Decimal::zero(),
                total: decimal_one(),
                locked: false
            }
        );

        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_chargeback_non_existent() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Chargeback(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
    }

    #[test]
    fn test_process_transaction_chargeback_other_client() {
        let mut account = Account::new(2);
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Chargeback(TransactionWithoutAmount { client: 2, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(2));
        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_chargeback_not_disputed() {
        let mut account = Account::new(1);
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Chargeback(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(account, Account::new(1));
        assert_eq!(txs_storage.len(), 1);
        assert_eq!(
            txs_storage.get(&1).unwrap(),
            &StorageTransaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            })
        );
    }

    #[test]
    fn test_process_transaction_chargeback() {
        let mut account = Account::new(1);
        account.available = Decimal::zero();
        account.held = decimal_one();
        account.total = decimal_one();
        let mut txs_storage = HashMap::new();
        txs_storage.insert(
            1,
            StorageTransaction::DisputeInProgress(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: decimal_one(),
            }),
        );

        process_transaction(
            &mut txs_storage,
            1,
            Transaction::Chargeback(TransactionWithoutAmount { client: 1, tx: 1 }),
            &mut account,
        );

        assert_eq!(
            account,
            Account {
                client: 1,
                available: Decimal::zero(),
                held: Decimal::zero(),
                total: Decimal::zero(),
                locked: true
            }
        );

        assert!(txs_storage.get(&1).is_none());
    }

    fn decimal_one() -> Decimal {
        Decimal::from_f64(1.0).unwrap()
    }
}
