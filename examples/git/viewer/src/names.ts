export const canonicalName = (name: string) => (name.endsWith(".git") ? name : `${name}.git`);

export function repositoryName(input: string, base: string): string | undefined {
  const name = canonicalName(input);
  if (
    !input ||
    input === "*" ||
    name === ".git" ||
    name.length > 4096 ||
    /[\s%?#\\]/.test(input) ||
    input.split("/").some((part) => !part || part === "." || part === "..") ||
    new URL(`/${name}`, base).pathname !== `/${name}`
  )
    return undefined;
  return name;
}
