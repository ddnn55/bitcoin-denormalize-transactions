import sys
from blockchain_parser.blockchain import Blockchain

import sqlite3

conn = sqlite3.connect('book.sqlite3')
# conn = sqlite3.connect(':memory:')
c = conn.cursor()
c.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='unspent_outputs';''')
result = c.fetchone()
if result == None:
    
    print("Creating unspent_outputs table...")
    c.execute('''CREATE TABLE unspent_outputs
                 (tx_hash text, output_number int, address text, amount text)''')
    c.execute('CREATE UNIQUE INDEX tx_output ON unspent_outputs (tx_hash, output_number);')

    print("Creating balances table...")
    c.execute('''CREATE TABLE balances
                 (time text, address text, amount text)''')
    c.execute('CREATE UNIQUE INDEX point ON balances (time, address);')

def commit_db_and_exit():
    conn.commit()
    conn.close()
    exit()

import signal
def signal_handler(signal, frame):
        print('\nReceived SIGINT, commiting DB and quitting...')
        commit_db_and_exit()

signal.signal(signal.SIGINT, signal_handler)

insertions_since_last_commit = 0
total_outputs_inserted = 0
def store_output(timestamp, tx_hash, output_number, _output):
    # print(timestamp.timestamp())
    global insertions_since_last_commit
    global total_outputs_inserted
    # don't know how to handle outputs mentioning more than 1 address at the moment
    if len(_output.addresses) == 1:
        address = _output.addresses[0].address

        c.execute('INSERT INTO unspent_outputs VALUES(?, ?, ?, ?)', (tx_hash, output_number, address, _output.value))
        total_outputs_inserted = total_outputs_inserted + 1

        # update balance of address
        c.execute('SELECT * FROM balances WHERE address = ? ORDER BY time DESC LIMIT 1', (address,))
        result = c.fetchone()
        if result != None:
            print("Found previous balance %s" % (result,))
            print("TODO: cumulate new balance")
        else:
            c.execute('INSERT INTO balances VALUES(?, ?, ?)', (timestamp.isoformat(), address, _output.value))

        insertions_since_last_commit = insertions_since_last_commit + 1
        if insertions_since_last_commit > 10000:
            conn.commit()
            insertions_since_last_commit = 0
    elif len(_output.addresses) == 0:
        if _output.value != 0:
            print("Info: 0 address output. tx=%s output_number=%s output=%s" % (tx_hash, output_number, _output))
            print("...of value = %s satoshis" % (_output.value))
            print("...quitting, we should handle this right?")
            commit_db_and_exit()
    else:
        print("multi address output")
        print(_output.addresses)
        commit_db_and_exit()

def process_input(_input):
    # print(_input)
    c.execute('SELECT address, amount FROM unspent_outputs WHERE tx_hash=? AND output_number=?', (_input.transaction_hash, _input.transaction_index))
    result = c.fetchone()
    if result != None:
        # print("Found %s" % (result,))
        # an output can only be spent through an input once! we can remove this row!
        c.execute('DELETE FROM unspent_outputs WHERE tx_hash=? AND output_number=?', (_input.transaction_hash, _input.transaction_index))

def get_number_of_unspent_outputs():
    c.execute('SELECT COUNT(*) FROM unspent_outputs')
    return c.fetchone()[0]

# Instantiate the Blockchain by giving the path to the directory 
# containing the .blk files created by bitcoind
tx_index = 0
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
            input_address = process_input(_input)
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
            store_output(block.header.timestamp, tx.hash, no, output)
            # print("tx=%s outputno=%d type=%s value=%s" % (tx.hash, no, output.addresses, output.value))
        
        tx_index = tx_index + 1
        if tx_index % 1000 == 0:
            print("unspent outputs %s / %s total outputs" % (get_number_of_unspent_outputs(), total_outputs_inserted))
