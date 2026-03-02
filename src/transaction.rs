use crate::parsing::{CsvTransaction, TxType};
use rust_decimal::Decimal;

#[derive(Clone)]
pub struct TransactionWithAmount {
    pub client: u16,
    pub tx: u32,
    pub amount: Decimal,
}

pub struct TransactionWithoutAmount {
    pub client: u16,
    pub tx: u32,
}

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
