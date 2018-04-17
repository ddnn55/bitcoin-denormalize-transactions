#!/usr/bin/env python3

import sys
from blockchain_parser.blockchain import Blockchain
from operator import attrgetter

import sqlite3
import json


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

conn = None
c = None
def open_db():
    global conn
    global c
    # conn = sqlite3.connect('balances.sqlite3')
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()
open_db()

def query_execute(query, args=(), explain=False):
    global c
    if explain:
        explain_query = 'EXPLAIN QUERY PLAN ' + query
        c.execute(explain_query, args)
        eprint("explanation of " + query)
        eprint(c.fetchone())
    c.execute(query, args)

query_execute('''SELECT name FROM sqlite_master WHERE type='table' AND name='unspent_outputs';''')
result = c.fetchone()
if result == None:
    
    eprint("Creating unspent_outputs table...")
    query_execute('''CREATE TABLE unspent_outputs
                 (tx_hash text, output_number int, address text, amount int, multi_address int)''')
    query_execute('CREATE UNIQUE INDEX tx_output ON unspent_outputs (tx_hash, output_number);')

    eprint("Creating balances table...")
    query_execute('''CREATE TABLE balances
                 (address text, amount int)''')
    query_execute('CREATE UNIQUE INDEX point ON balances (address);')

def commit_db_and_exit():
    conn.commit()
    conn.close()
    exit()

import signal
def signal_handler(signal, frame):
        eprint('\nReceived SIGINT, commiting DB and quitting...')
        commit_db_and_exit()

signal.signal(signal.SIGINT, signal_handler)



insertions_since_last_commit = 0
total_outputs_inserted = 0

def process_output(output, output_number, tx_hash):
    global insertions_since_last_commit
    global total_outputs_inserted

    value = output.value

    multi_address = len(output.addresses) > 1
    if multi_address:
        # well, it's a multi-address output. strictly speaking,
        # we should debit all these addresses if this output
        # is ever spent. this does happen, although seems infrequent
        # anecdotally in the first 100 blk files.
        # could become more frequent in subsequent blk files as service providers
        # offer more features like multi sig required, or multi sigs capable
        eprint("WARNING: %s output %s is multi-address. We don't accurately handle multi-address output. If output is spent, balances of these addresses will be too high, except for the balance of the address that spent it." % (tx_hash, output_number))

    for address in output.addresses:
        address = address.address
        row = (tx_hash, output_number, address, value, multi_address)
        try:
            query_execute('INSERT INTO unspent_outputs VALUES(?, ?, ?, ?, ?)', row)
        except sqlite3.IntegrityError as e:
            eprint(e)
            eprint("WARNING: ignoring transaction with duplicate hash. should be extremely rare. see https://bitcoin.stackexchange.com/questions/11999/can-the-outputs-of-transactions-with-duplicate-hashes-be-spent")
            eprint(row)
        total_outputs_inserted = total_outputs_inserted + 1

        # update balance of address
        query_execute('SELECT * FROM balances WHERE address = ?', (address,), explain=False)
        result = c.fetchone()
        # eprint(result)
        to_balance = None
        if result != None:
            # eprint("Found previous balance %s" % (result,))
            to_balance = result[1] + value

            # if result[0] == timestamp.isoformat():
            query_execute('UPDATE balances SET amount = ? WHERE address = ?', (to_balance, address), explain=False)
                
        else:
            # no previous balance record. set balance to value
            to_balance = value
            try:
                query_execute('INSERT INTO balances VALUES(?, ?)', (address, to_balance))
            except sqlite3.IntegrityError as e:
                eprint(e)
                eprint("while trying to insert %s" % ((address, to_balance),))
                commit_db_and_exit()
        
        # periodically flush DB
        insertions_since_last_commit = insertions_since_last_commit + 1
        if insertions_since_last_commit > 10000:
            conn.commit()
            # will closing and re-opening plug a memory leak?
            # conn.close()
            # open_db()
            # eprint("Closed and re-opened db")
            insertions_since_last_commit = 0

    
def process_transfer(timestamp, debits, _credits, value, tx_hash):
    print(json.dumps([
        timestamp.isoformat(),
        debits,
        _credits,
        value,
        # tx_hash
    ]))
    # print("%s,%s,%s,%s,%s" % (timestamp.isoformat(), inputs, to_address, value, to_balance))


# def process_output(timestamp, inputs, tx_hash, output_number, _output):

    # global devnull_address

    # if len(_output.addresses) == 1:
    #     process_transfer(timestamp, inputs, _output.addresses[0].address, _output.value, tx_hash, output_number)

    # elif len(_output.addresses) == 0:
    #     if _output.value != 0:
    #         eprint("Info: 0 address output. tx=%s output_number=%s output=%s" % (tx_hash, output_number, _output))
    #         eprint("...of value = %s satoshis" % (_output.value))
    #         pseudo_address = next(devnull_address)
    #         eprint("...crediting to " + pseudo_address + " LOL")
    #         process_transfer(timestamp, inputs, pseudo_address, _output.value, tx_hash, output_number)
    #         # eprint("...quitting, we should handle this right?")
    #         # commit_db_and_exit()
    # else:
    #     eprint("multi address output. depositing to all addresses and flagging outputs as multi address")
    #     eprint(_output.addresses)
    #     eprint("Value:")
    #     eprint(_output.value)
    #     eprint("Type:")
    #     eprint(_output.type)

    #     for output_address in _output.addresses:
    #         process_transfer(timestamp, inputs, output_address.address, _output.value, tx_hash, output_number, multi_address=True)

    



def process_input(_input):
    # eprint(_input)
    query_execute('SELECT address, amount FROM unspent_outputs WHERE tx_hash=? AND output_number=?', (_input.transaction_hash, _input.transaction_index), explain=False)
    result = c.fetchone()
    # eprint(result)
    if result != None:
        (address, amount) = result
        # eprint("Found %s" % (result,))
        # an output can only be spent through an input once! we can remove this row!
        query_execute('DELETE FROM unspent_outputs WHERE tx_hash=? AND output_number=?', (_input.transaction_hash, _input.transaction_index), explain=False)
        # eprint(result[0])

        # debit account
        query_execute('SELECT * FROM balances WHERE address = ?', (address,), explain=False)
        balance = c.fetchone()[1]
        query_execute('UPDATE balances SET amount = ? WHERE address = ?', (balance - amount, address), explain=False)

        return result
    else:
        return None

def get_number_of_unspent_outputs():
    query_execute('SELECT COUNT(*) FROM unspent_outputs', explain=False)
    result = c.fetchone()[0]
    # eprint(result)
    return result

# Instantiate the Blockchain by giving the path to the directory 
# containing the .blk files created by bitcoind
tx_index = 0
eprint("Initializing blockchain at " + sys.argv[1])
blockchain = Blockchain(sys.argv[1])
eprint("Initialized. Loading blocks...")
unordered_blocks = []
for b, block in enumerate(blockchain.get_unordered_blocks()):
    unordered_blocks.append(block)
    if b % 1000 == 0:
        eprint("Loaded " + str(b) + " blocks", end="\r")
total_blocks = len(unordered_blocks)
eprint("Loaded all " + str(total_outputs_inserted) + " blocks. Sorting by time...")
blocks = sorted(unordered_blocks, key=attrgetter('header.timestamp'))
unordered_blocks = None
eprint("Sorted.")

tx_count = 0
output_count = 0

block_number = 0

num_outputs_hist = {}

# from pympler.tracker import SummaryTracker
# tracker = SummaryTracker()

print("time,from_address,to_address,amount_satoshis,to_balance")

while len(blocks) > 0:
    block = blocks.pop(0)
# for block_number, block in enumerate(blocks):
    # eprint("block %s / %s: %s" % (block_number, len(blocks), block.header.timestamp.isoformat(),))

    # sys.stderr.write('block hash: ' + block.hash + ' ' + str(block.n_transactions) + '\n')

    for tx in block.transactions:

        tx_count = tx_count + 1

        # example input referencing a previous output
        #
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b input_num=0 input=Input(06b617171d45dccf383bf82a0cf5cd5a142d6a9bf91393223d46f041bf0380b4,78)
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=0 type=[Address(addr=144vPbnRe5ETJiRLqbVtEUqtgwAAb1hJLw)] value=316327
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=1 type=[Address(addr=16tBxGUb95wa7yhkAn3uSU125ZxWzqFK8g)] value=3673759
        # tx=66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b outputno=2 type=[Address(addr=1DiUJ9PXnump3KHsjc1qsp3E3LUbPvAxR4)] value=7574557
        # tx=635745100dc559787dc3b09bd31a62155084dc582aff741debb18828b91a8ec0 input_num=0 input=Input(66f701374a05702aa1ab920486cfc1b7a1f643b6ed75c04f2583d412f12ff88b,2)

        debits = []
        # eprint("input addresses:")
        for no, _input in enumerate(tx.inputs):
            debit = process_input(_input)
            if debit != None:
                debits.append(debit)
                # eprint(input_address)
            # eprint("tx=%s input_num=%s input=%s" % (tx.hash, no, _input))

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
        
        if not tx.n_outputs in num_outputs_hist:
            num_outputs_hist[tx.n_outputs] = 0
        num_outputs_hist[tx.n_outputs] = num_outputs_hist[tx.n_outputs] + 1

        _credits = []
        for no, output in enumerate(tx.outputs):
            output_count = output_count + 1
        #     if tx.hash == "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599":
        #         eprint("processing tx d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599 output #" + str(no))
            process_output(output, no, tx.hash)
            # process_output(block.header.timestamp, inputs, tx.hash, no, output)
            num_outputs = 0
            for address in output.addresses:
                _credits.append((address.address, output.value))
                num_outputs = num_outputs + 1
            # eprint("tx=%s outputno=%d type=%s value=%s" % (tx.hash, no, output.addresses, output.value))
        
        value = sum([credit[1] for credit in _credits])

        process_transfer(block.header.timestamp, debits, _credits, value, tx.hash)
        # process_transfer(block.header.timestamp, debits, _credits, value, tx_hash, output_number, multi_address = False)

        tx_index = tx_index + 1
        if tx_index % 1000 == 0:
            percent = round(100 * block_number / total_blocks)
            eprint("\r%s%%" % (percent,), end="")
            # eprint("\r%s%% done. unspent outputs %s / %s total outputs; %s" % (percent, get_number_of_unspent_outputs(), total_outputs_inserted, num_outputs_hist), end="")
            # eprint(num_outputs_hist)
        
        # eprint("--------------------------------------------\n\n\n")
            
    # if(block_number % 1000 == 0):
    #     eprint("Finished block " + str(block_number))
    #     tracker.print_diff()

    block_number = block_number + 1

eprint("Processed %s transactions and %s outputs." % (tx_count, output_count))
