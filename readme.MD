# FastSync

## Fast syncronization across networks using speedy compression, lots of parallelization and fast hashmaps for keeping track of things internally


[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/lkarlslund/fastsync)](https://github.com/lkarlslund/fastsync/releases/latest) ![GitHub all releases](https://img.shields.io/github/downloads/lkarlslund/fastsync/total) ![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lkarlslund/fastsync/prerelease.yml?branch=main)

I made this because a customer asked me to transfer 100TB from one system to another. The data were raw backups with billions of files that were hardlinked together. Using rsync it seemed to progress very slowly and not load the RAID system enough - and it would always end badly with the machine running out of memory

### VERIFY YOUR DATA AFTER USING

*this is just something I whisked together, so handle with care*

Features:

- server and client - sends files from server to client
- preserves timestamps, owner UID, group GID, attributes
- handles character devices, hardlinks, softlinks etc.
- compresses data over the wire using snappy compression
- very performant - I've seen speeds up to ~90K files processed/sec when resyncing

FastSync consists of:

## Server mode

Start up the source side, listening for unauthenticated clients

```bash
fastsync [--directory /your/source/directory] [--bind 0.0.0.0:7331] server
```

## Client mode

Connects to the server and starts syncing files to the client

```bash
fastsync [--hardlinks true] [--checksum false] [--delete false] [--acl true] [--pfile 4096] [--pdir 512] [--loglevel info] [--blocksize 131072] [--statsinterval 5] [--queueinterval 30] [--directory /your/target/directory] [--bind serverip:7331] client
```

Options:

- ```pfile``` sets the number of parallel file IO operations, for large RAID systems with lots of drives or flash storage the default 4096 is probably okay, but expect major load on both systems
- ```pdir``` sets the number of parallel directory listing operations

- ```checksum``` forces fastsync to check all data on all existing files using checksums for every block (otherwise it assumes files with same size, timestamp and attributes are equal)

- ```hardlinks``` enables keeping the same files hardlinked across the network, this is default enabled, and should do no harm even if you don't use hardlinks

- ```loglevel``` sets the verbosity, you can use error, info, debug and trace

- ```blocksize``` is the number of bytes to checksum and the size of the data blocks transferred across the network. If you increase this too much, the RPC traffic will get "choppy" and the parallelization will suffer. If you're running on gigabit the default is probably fine, but if it's 10Gbps I'd probably increase this

- ```statsinterval``` is how often to output performance data, set to 0 to disable

- ```queueinterval``` is how often to output internal queue data, set to 0 to disable (mostly for debugging)
