# bitcoin-denormalize-transactions
Consumes bitcoin blk*.dat files and produces (from_address, to_address, amount, time) tuples

## Dependencies

* Fetch test block data `./fetch_test_data.sh`
* https://github.com/alecalve/python-bitcoin-blockchain-parser

## TODO

1. Deduce input addresses. An input appears to be a reference to an output of a previous transaction:
```
$ python3 bnt.py test | grep 66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b
tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b input=Input(06b617171d45dccf383bf82a0cf5cd5a142d6a9bf91393223d46f041bf0380b4,78)
tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=0 type=[Address(addr=144vPbnRe5ETJiRLqbVtEUqtgwAAb1hJLw)] value=316327
tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=1 type=[Address(addr=16tBxGUb95wa7yhkAn3uSU125ZxWzqFK8g)] value=3673759
tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=2 type=[Address(addr=1DiUJ9PXnump3KHsjc1qsp3E3LUbPvAxR4)] value=7574557
tx=635745100dc559787dc3b09bd31a62155084dc582aff741debb18828b91a8ec0 input=Input(66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b,2)
```

2. Deduce transaction time. See: https://bitcoin.org/en/developer-guide#locktime-and-sequence-number

3. Given (1) and (2), seems relatively straightforward to produce `(input_addresses[], output_addresses[], amount, time)` tuples, but do we want to, and can we reduce a multi-input, multi-output transaction to a single input, single output transaction? I believe I heard that a simple single-from, single-to transaction often or usually ends up being implemented as a multi-from, multi-to transaction due to, you know, computers.
