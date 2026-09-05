fn main() -> anyhow::Result<()> {
    let file = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("pass a component path"))?;
    let bytes = std::fs::read(file)?;
    let mut depth = 0;
    for payload in wasmparser::Parser::new(0).parse_all(&bytes) {
        match payload? {
            wasmparser::Payload::Version { .. } => depth += 1,
            wasmparser::Payload::End(_) => depth -= 1,
            wasmparser::Payload::ComponentImportSection(imports) if depth == 1 => {
                for import in imports {
                    println!("import {}", import?.name.0);
                }
            }
            wasmparser::Payload::ComponentExportSection(exports) if depth == 1 => {
                for export in exports {
                    println!("export {}", export?.name.0);
                }
            }
            _ => {}
        }
    }
    Ok(())
}
