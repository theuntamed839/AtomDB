# DataStore4J

**DataStore4J** A thread-safe, high-performance key-value database built on an LSM-tree architecture, inspired by [LevelDB](https://github.com/google/leveldb).

## ⚙️ Architecture Overview

- DataStore4J uses a **Log-Structured Merge Tree (LSM)** architecture.
- Incoming key-value pairs are first stored in an **in-memory structure (memtable)**.
- Once full, the memtable is flushed to disk as a new **SSTable** (Sorted String Table) file.
- **SSTables are immutable**, meaning that keys are never updated in place.

## 🗑️ Deletes and Tombstones

- When a key is deleted, a **tombstone** marker is written instead of removing the entry.
- Obsolete data (old values and tombstones) accumulates over time and must be cleared.

## 🧹 Compaction

- **Compaction** is a background process that:
    - Merges multiple SSTables.
    - Eliminates duplicate or deleted entries.
    - Reduces disk usage and improves read performance.

## 🔍 Reads

- Reads proceed **from the newest SSTables to the oldest**, ensuring that the latest value is found first.

---

Feel free to contribute or explore further!
