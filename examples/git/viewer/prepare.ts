await Bun.write(
  "build/wit/wal.wit",
  `${await Bun.file("../../wal-component/wit/wal.wit").text()}\nworld reader { import wal; }\n`,
);
const types = Bun.spawn(
  ["bun", "x", "--bun", "jco", "types", "build/wit", "-n", "reader", "-o", "build/types"],
  { stdout: "inherit", stderr: "inherit" },
);
if (await types.exited) throw new Error("WAL type generation failed");

export {};
