# Rust Transactions Processing

Use the following command to run it (check `examples` folder for example `transactions.csv` files)

`cargo run -- transactions.csv > accounts.csv`

## Input (transactions.csv)

```csv
type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
```

## Output (accounts.csv)

```csv
client, available, held, total, locked
2, 2, 0, 2, false
1, 1.5, 0, 1.5, false
```
