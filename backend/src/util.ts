export function getConfig() {
  const dbPath = Deno.args[0];

  if (!dbPath) {
    console.error("Didn't provide dbPath in argumnts", Deno.args);
    Deno.exit(2);
  }

  return { "dbPath": dbPath };
}
