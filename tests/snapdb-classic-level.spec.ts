import { ClassicLevel } from "classic-level";
import { SnapDB } from "../src/snapdb";

describe('Name of the group', () => {
  it("should append correctly to the database", async () => {
    const db = new SnapDB(
      new ClassicLevel(`/tmp/snapdb-tests-${Math.floor(Math.random() * 10000)}`, {
        valueEncoding: "buffer",
      })
    );
    const hash = await db.append("John Doe");
    const data = await db.getAndDecode(hash);
    expect(data.value).toEqual("John Doe");
  });

  it("it should iterate through all entries without decoding", async () => {
    const db = new SnapDB(
      new ClassicLevel(`/tmp/snapdb-tests-${Math.floor(Math.random() * 10000)}`, {
        valueEncoding: "buffer",
      })
    );
    for (let i = 0; i < 30; i++) {
      await db.append("John Doe");
    }

    let index = 0;
    for await (let entry of db.readAll()) {
      const decoded = await SnapDB.decode(entry);
      expect(decoded.value).toEqual("John Doe");
      expect(decoded.index).toEqual(index);
      index++;
    }
  });

  it("it should iterate and decode all entries", async () => {
    const db = new SnapDB(
      new ClassicLevel(`/tmp/snapdb-tests-${Math.floor(Math.random() * 10000)}`, {
        valueEncoding: "buffer",
      })
    );
    for (let i = 0; i < 30; i++) {
      await db.append("John Doe");
    }

    for await (let entry of db.readAllAndDecode()) {
      expect(entry.value).toEqual("John Doe");
    }
  });

  it("it should validate if the last hash exists", async () => {
    const db = new SnapDB(
      new ClassicLevel(`/tmp/snapdb-tests-${Math.floor(Math.random() * 10000)}`, {
        valueEncoding: "buffer",
      })
    );

    expect(await db.getFirstHash()).toEqual(null);
    expect(await db.getLastHash()).toEqual(null);
    expect(await db.getCurrentIndex()).toEqual(null);

    expect(await db.has("the cake is a lie")).toEqual(false);
    const hash = await db.append("it");
    expect(await db.has(hash)).toEqual(true);

    expect(await db.getCurrentIndex()).toEqual(0);
    expect((await db.getFirstHash()) !== null).toBeTruthy();
    expect((await db.getFirstHash()) !== null).toBeTruthy();
  });

  it("it should read from a specific index until another index", async () => {
    const db = new SnapDB(
      new ClassicLevel(`/tmp/snapdb-tests-${Math.floor(Math.random() * 10000)}`, {
        valueEncoding: "buffer",
      })
    );
    for (let i = 0; i < 30; i++) {
      await db.append("John Doe");
    }

    let index = 3;
    for await (let entry of db.readFrom(3, 17)) {
      const decoded = await SnapDB.decode(entry);
      expect(decoded.value).toEqual("John Doe");
      expect(decoded.index).toEqual(index);
      index++;
    }
  });

  it("it should read and decode from a specific index until another index", async () => {
    const db = new SnapDB(
      new ClassicLevel(`/tmp/snapdb-tests-${Math.floor(Math.random() * 10000)}`, {
        valueEncoding: "buffer",
      })
    );
    for (let i = 0; i < 30; i++) {
      await db.append("John Doe");
    }

    let index = 3;
    for await (let entry of db.readFromAndDecode(3, 17)) {
      expect(entry.value).toEqual("John Doe");
      expect(entry.index).toEqual(index);
      index++;
    }
  });
});

