import * as crypto from "crypto";

import { AbstractLevel } from "abstract-level";
import { ClassicLevel } from "classic-level";
import { MemoryLevel } from "memory-level";

type Entry = {
  hash: string;
  index: number;
  value: any;
  previous: string | null;
  next: string | null;
};

type AbstractDb = AbstractLevel<any, any, any>;

export class SnapDB {
  private db!: AbstractDb;

  /**
   * Example:
   *
   * new SnapDB(new ClassicLevel('./data'))
   */
  private constructor(db: AbstractDb) {
    this.db = db;
  }

  static createInMemory() {
    return new SnapDB(
      new MemoryLevel({
        valueEncoding: "json",
      })
    );
  }

  static create(path: string) {
    return new SnapDB(
      new ClassicLevel(path, {
        valueEncoding: "json",
      })
    );
  }

  async getFirstHash(): Promise<any> {
    try {
      return await this.db.get("firstHash");
    } catch (error) {
      return null;
    }
  }

  async getLastHash(): Promise<string | null> {
    try {
      return await this.db.get("lastHash");
    } catch (error) {
      return null;
    }
  }

  async getCurrentIndex(): Promise<number | null> {
    const lastHash = await this.getLastHash();

    if (!lastHash) {
      return null;
    }

    const lastEntry = await this.get(lastHash);

    return lastEntry.index;
  }

  async append(value: any): Promise<string> {
    const hash = this.randomHash();
    const firstHash = await this.getFirstHash();

    let batch = this.db.batch();

    if (!firstHash) {
      batch = batch.put("firstHash", hash);
    }

    const lastHash = await this.getLastHash();

    if (lastHash) {
      const lastEntry = await this.get(lastHash);
      batch = batch.put(lastHash, {
        ...lastEntry,
        next: hash,
      });
    }

    const currentIndex = await this.getCurrentIndex();
    const index = typeof currentIndex === "number" ? currentIndex + 1 : 0;

    await batch
      .put("lastHash", hash)
      .put(index.toString(), hash)
      .put(hash, {
        hash,
        index,
        value,
        previous: lastHash,
        next: null,
      })
      .write();

    return hash;
  }

  async get(hash: string): Promise<Entry> {
    return this.db.get(hash);
  }

  async has(hash: string): Promise<boolean> {
    try {
      await this.get(hash);
      return true;
    } catch (error) {
      return false;
    }
  }

  async *readFrom(start: number, finish: number) {
    const pointer = await this.get(start.toString());
    const entry = await this.db.get(pointer);

    let next = entry.hash;

    while (next !== null) {
      const entry = await this.get(next);
      yield entry;

      const parsedEntry = entry;
      next = parsedEntry.next;
      if (parsedEntry.index >= finish) {
        break;
      }
    }
  }

  async *readAll() {
    let next = await this.getFirstHash();
    while (next !== null) {
      const entry = await this.get(next);
      next = entry.next;

      yield entry;
    }
  }

  randomHash(): string {
    return crypto
      .createHash("sha256")
      .update(crypto.randomBytes(32))
      .digest("hex");
  }
}
