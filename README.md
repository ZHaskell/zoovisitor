# ZooVisitor

[![Hackage](https://img.shields.io/hackage/v/zoovisitor.svg?style=flat)](https://hackage.haskell.org/package/zoovisitor)

An Apache [zookeeper](https://zookeeper.apache.org/) client for Haskell.

Tested with libzookeeper-mt [3.4, 3.6], a higher version should work but not
guaranteed.

**NOTE: this library is still in development.**

## Features

- Simple

  - Does not require knowledge of the C APIs
  - Does not require malloc and free memory manually
  - High-level api written in Haskell

- Safe

  - Type safe
  - A failure action should throw an exception

- Fast
  - All underlying calls are non-blocking, you can use Haskell lightweight
    threads to speed up your tasks.
  - The speed should be compatible to C implementation
