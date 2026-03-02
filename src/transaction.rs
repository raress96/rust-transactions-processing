use crate::parsing::{CsvTransaction, TxType};
use rust_decimal::Decimal;

#[derive(Debug, PartialEq, Clone)]
pub struct TransactionWithAmount {
    pub client: u16,
    pub tx: u32,
    pub amount: Decimal,
}

#[derive(Debug, PartialEq)]
pub struct TransactionWithoutAmount {
    pub client: u16,
    pub tx: u32,
}

#[derive(Debug, PartialEq)]
pub enum Transaction {
    Deposit(TransactionWithAmount),
    Withdrawal(TransactionWithAmount),
    Dispute(TransactionWithoutAmount),
    Resolve(TransactionWithoutAmount),
    Chargeback(TransactionWithoutAmount),
}

impl TryFrom<CsvTransaction> for Transaction {
    type Error = String;

    fn try_from(csv_tx: CsvTransaction) -> Result<Self, Self::Error> {
        match csv_tx.tx_type {
            TxType::Deposit => Ok(Transaction::Deposit(TransactionWithAmount {
                client: csv_tx.client,
                tx: csv_tx.tx,
                amount: csv_tx
                    .amount
                    .ok_or_else(|| format!("Deposit tx {} missing amount", csv_tx.tx))?,
            })),
            TxType::Withdrawal => Ok(Transaction::Withdrawal(TransactionWithAmount {
                client: csv_tx.client,
                tx: csv_tx.tx,
                amount: csv_tx
                    .amount
                    .ok_or_else(|| format!("Withdrawal tx {} missing amount", csv_tx.tx))?,
            })),
            TxType::Dispute => {
                if csv_tx.amount.is_some() {
                    return Err(format!("Dispute tx {} should not have amount", csv_tx.tx));
                }

                Ok(Transaction::Dispute(TransactionWithoutAmount {
                    client: csv_tx.client,
                    tx: csv_tx.tx,
                }))
            }
            TxType::Resolve => {
                if csv_tx.amount.is_some() {
                    return Err(format!("Resolve tx {} should not have amount", csv_tx.tx));
                }

                Ok(Transaction::Resolve(TransactionWithoutAmount {
                    client: csv_tx.client,
                    tx: csv_tx.tx,
                }))
            }
            TxType::Chargeback => {
                if csv_tx.amount.is_some() {
                    return Err(format!(
                        "Chargeback tx {} should not have amount",
                        csv_tx.tx
                    ));
                }

                Ok(Transaction::Chargeback(TransactionWithoutAmount {
                    client: csv_tx.client,
                    tx: csv_tx.tx,
                }))
            }
        }
    }
}

impl Transaction {
    pub fn client(&self) -> u16 {
        match self {
            Transaction::Deposit(t) | Transaction::Withdrawal(t) => t.client,
            Transaction::Dispute(t) | Transaction::Resolve(t) | Transaction::Chargeback(t) => {
                t.client
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromPrimitive;

    #[test]
    fn test_try_from_deposit() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_ok());
        assert_eq!(
            tx.unwrap(),
            Transaction::Deposit(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: Decimal::from_f64(1.0).unwrap()
            })
        );
    }

    #[test]
    fn test_try_from_deposit_err() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Deposit,
            client: 1,
            tx: 1,
            amount: None,
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_err());
        assert_eq!(tx.unwrap_err(), "Deposit tx 1 missing amount");
    }

    #[test]
    fn test_try_from_withdrawal() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Withdrawal,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_ok());
        assert_eq!(
            tx.unwrap(),
            Transaction::Withdrawal(TransactionWithAmount {
                client: 1,
                tx: 1,
                amount: Decimal::from_f64(1.0).unwrap()
            })
        );
    }

    #[test]
    fn test_try_from_withdrawal_err() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Withdrawal,
            client: 1,
            tx: 1,
            amount: None,
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_err());
        assert_eq!(tx.unwrap_err(), "Withdrawal tx 1 missing amount");
    }

    #[test]
    fn test_try_from_dispute() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_ok());
        assert_eq!(
            tx.unwrap(),
            Transaction::Dispute(TransactionWithoutAmount {
                client: 1,
                tx: 1,
            })
        );
    }

    #[test]
    fn test_try_from_dispute_err() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Dispute,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_err());
        assert_eq!(tx.unwrap_err(), "Dispute tx 1 should not have amount");
    }

    #[test]
    fn test_try_from_resolve() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_ok());
        assert_eq!(
            tx.unwrap(),
            Transaction::Resolve(TransactionWithoutAmount {
                client: 1,
                tx: 1,
            })
        );
    }

    #[test]
    fn test_try_from_resolve_err() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Resolve,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_err());
        assert_eq!(tx.unwrap_err(), "Resolve tx 1 should not have amount");
    }

    #[test]
    fn test_try_from_chargeback() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_ok());
        assert_eq!(
            tx.unwrap(),
            Transaction::Chargeback(TransactionWithoutAmount {
                client: 1,
                tx: 1,
            })
        );
    }

    #[test]
    fn test_try_from_chargeback_err() {
        let csv_tx = CsvTransaction {
            tx_type: TxType::Chargeback,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        };

        let tx = Transaction::try_from(csv_tx);

        assert!(tx.is_err());
        assert_eq!(
            tx.unwrap_err(),
            "Chargeback tx 1 should not have amount"
        );
    }
}
