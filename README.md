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

## Implementation notes

- CSV isn't loaded all in memory, it's read line by line
- typed structs and enums are used for handling validation
- client accounts are simply stored in memory
- only relevant transactions for disputes are stored in memory

## Assumptions

- Client and tx ids are unique
- If an account is locked, deposits and withdrawals are rejected
- Withdrawals exceeding available funds are rejected
- Only deposits can be disputed, not withdrawals
- Invalid transactions are logged and not processed, they do not fail the whole processing

## AI Usage disclaimer

Claude Code was used sparingly to help with debugging and speeding up the implementation of features that depend on 3rd party crates,
as well as for help with implementing tests similarly to already existing user defined tests.

It was NOT used for making any decision regarding the architecture, code organization or general logic of the application.
