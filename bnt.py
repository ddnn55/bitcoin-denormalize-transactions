import sys
from blockchain_parser.blockchain import Blockchain

import sqlite3

conn = sqlite3.connect('book.sqlite3')
c = conn.cursor()
c.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='outputs';''')
result = c.fetchone()
if result == None:
    print("Creating table...")
    c.execute('''CREATE TABLE outputs
                 (tx_hash text, output_number int, address text)''')
    c.execute('CREATE UNIQUE INDEX tx_output ON outputs (tx_hash, output_number);')

def store_output(tx_hash, output_number, _output):
    # don't know how to handle outputs mentioning more than 1 address at the moment
    assert(len(_output.addresses) == 1)

    c.execute('INSERT INTO outputs VALUES(?, ?, ?)', (tx_hash, output_number, _output.addresses[0].address))

def lookup_address_of_input(_input):
    # print(_input)
    c.execute('SELECT address FROM outputs WHERE tx_hash=? AND output_number=?', (_input.transaction_hash, _input.transaction_index))
    print(c.fetchone())

# Instantiate the Blockchain by giving the path to the directory 
# containing the .blk files created by bitcoind
blockchain = Blockchain(sys.argv[1])
for block in blockchain.get_unordered_blocks():
    for tx in block.transactions:

        # example input referencing a previous output
        #
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b input_num=0 input=Input(06b617171d45dccf383bf82a0cf5cd5a142d6a9bf91393223d46f041bf0380b4,78)
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=0 type=[Address(addr=144vPbnRe5ETJiRLqbVtEUqtgwAAb1hJLw)] value=316327
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=1 type=[Address(addr=16tBxGUb95wa7yhkAn3uSU125ZxWzqFK8g)] value=3673759
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=2 type=[Address(addr=1DiUJ9PXnump3KHsjc1qsp3E3LUbPvAxR4)] value=7574557
        # tx=635745100dc559787dc3b09bd31a62155084dc582aff741debb18828b91a8ec0 input_num=0 input=Input(66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b,2)

        for no, _input in enumerate(tx.inputs):
            input_address = lookup_address_of_input(_input)
            # print("tx=%s input_num=%s input=%s" % (tx.hash, no, _input))

        # so, 1 or more outputs may be change wallets, wallets "secretly"
        # created by the wallet client to receive change, since only entire previous
        # outputs can be inputs.
        # meaning it's not straightforward to maintain a balance for every "human"
        # wallet, in fact hierarchichal deterministic wallets like Jaxx skip to a new
        # real wallet address with every transaction "as a way to make your balance" private
        # but can we not, say, when a transaction has multiple inputs, assume
        # with reasonable confidence that they all belong to the same human?
        # that would still only let us associate some change/privacy wallets with
        # each other, and maybe wallet clients do lots of stuff to retain privacy
        # in the long run, e.g. even after everything is spent
        
        for no, output in enumerate(tx.outputs):
            store_output(tx.hash, no, output)
            print("tx=%s outputno=%d type=%s value=%s" % (tx.hash, no, output.addresses, output.value))
