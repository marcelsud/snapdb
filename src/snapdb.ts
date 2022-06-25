import * as cbor from "cbor";
import * as crypto from "crypto";

import { AbstractLevel } from 'abstract-level';

type Entry = {
  hash: string;
  index: number;
  value: any;
  previous: string | null;
  next: string | null;
}

type AbstractDb = AbstractLevel<any, any, any>

export class SnapDB {
  private db!: AbstractDb;

  /**
   * Example:
   *
   * new SnapDB(new ClassicLevel('./data'))
   */
  constructor(db: AbstractDb) {
    this.db = db;
  }

  static async encode(data: any) {
    return cbor.encode(data);
  }

  static async decode(data: Buffer) {
    return cbor.decodeFirst(data);
  }

  async getFirstHash(): Promise<any> {
    try {
      const entry = await this.db.get("firstHash");
      return SnapDB.decode(entry);
    } catch (error) {
      return null;
    }
  }

  async getLastHash(): Promise<string | null> {
    try {
      const entry = await this.db.get("lastHash");
      return SnapDB.decode(entry);
    } catch (error) {
      return null;
    }
  }

  async getCurrentIndex(): Promise<number | null> {
    const lastHash = await this.getLastHash();

    if (!lastHash) {
      return null;
    }

    const lastEntry = await this.getAndDecode(lastHash);

    return lastEntry.index;
  }

  async append(value: any): Promise<string> {
    const hash = this.randomHash();
    const firstHash = await this.getFirstHash();

    let batch = this.db.batch();

    if (!firstHash) {
      batch = batch.put("firstHash", await SnapDB.encode(hash));
    }

    const lastHash = await this.getLastHash();

    if (lastHash) {
      const lastEntry = await this.getAndDecode(lastHash);
      batch = batch.put(
        lastHash,
        await SnapDB.encode({
          ...lastEntry,
          next: hash,
        })
      );
    }

    const currentIndex = await this.getCurrentIndex()
    const index = typeof currentIndex === 'number' ? currentIndex + 1 : 0;

    await batch
      .put("lastHash", await SnapDB.encode(hash))
      .put(index.toString(), await SnapDB.encode(hash))
      .put(
        hash,
        await SnapDB.encode({
          hash,
          index,
          value,
          previous: lastHash,
          next: null,
        })
      )
      .write();

    return hash;
  }

  async get(hash: string): Promise<Buffer> {
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

  async getAndDecode(hash: string): Promise<Entry> {
    return SnapDB.decode(await this.get(hash));
  }

  async *readFrom(start: number, finish: number) {
    const pointer = await SnapDB.decode(await this.get(start.toString()));
    const entry = await this.getAndDecode(pointer);

    let next = entry.hash;

    while (next !== null) {
      const entry = await this.get(next);
      yield entry;

      const parsedEntry = await SnapDB.decode(entry);
      next = parsedEntry.next;
      if (parsedEntry.index >= finish) {
        break;
      }
    }
  }

  async *readFromAndDecode(start: number, finish: number) {
    for await (let entry of this.readFrom(start, finish)) {
      yield SnapDB.decode(entry);
    }
  }

  async *readAll() {
    let next = await this.getFirstHash();
    while (next !== null) {
      const entry = await this.get(next);
      next = (await SnapDB.decode(entry)).next;

      yield entry;
    }
  }

  async *readAllAndDecode() {
    for await (let entry of this.readAll()) {
      yield SnapDB.decode(entry);
    }
  }

  randomHash(): string {
    return crypto
      .createHash("sha256")
      .update(crypto.randomBytes(32))
      .digest("hex");
  }
}
