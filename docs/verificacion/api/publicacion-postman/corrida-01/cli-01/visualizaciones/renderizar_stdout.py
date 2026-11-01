"""Renderiza stdout ya registrado; no ejecuta Postman ni peticiones de red."""

import hashlib
import html
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

SOURCE = (
    "docs/verificacion/api/publicacion-postman/corrida-01/cli-01/"
    "04-postman-stdout.txt"
)
SVG_NS = "http://www.w3.org/2000/svg"


def sha256(path: Path) -> str:
    """Devuelve el SHA-256 de los bytes originales de un archivo."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    """Genera SVG y manifiesto, verificando texto e inmutabilidad de fuentes."""
    destination = Path(__file__).resolve().parent
    run_dir = destination.parent
    sources = {p.name: sha256(p) for p in run_dir.iterdir() if p.is_file()}
    stdout = (run_dir / "04-postman-stdout.txt").read_bytes().decode("utf-8")
    lines = stdout.split("\n")
    width = max(1600, max(map(len, lines)) * 13 + 64)
    height = 120 + len(lines) * 27 + 110
    ET.register_namespace("", SVG_NS)
    root = ET.Element(f"{{{SVG_NS}}}svg", {
        "width": str(width), "height": str(height),
        "viewBox": f"0 0 {width} {height}",
        "role": "img", "aria-label": "Salida de Postman CLI — CP-04",
    })
    ET.SubElement(root, f"{{{SVG_NS}}}title").text = "Salida de Postman CLI — CP-04"
    ET.SubElement(root, f"{{{SVG_NS}}}rect", {
        "width": "100%", "height": "100%", "fill": "#ffffff",
    })

    def text_row(content: str, y: int, size: int = 20,
                 kind: str = "metadata") -> None:
        element = ET.SubElement(root, f"{{{SVG_NS}}}text", {
            "x": "32", "y": str(y), "font-size": str(size),
            "font-family": "DejaVu Sans Mono, monospace", "fill": "#111827",
            "class": kind, "{http://www.w3.org/XML/1998/namespace}space": "preserve",
        })
        element.text = content

    text_row("Salida de Postman CLI — CP-04", 36, 23)
    text_row("Publicación de un pedido en Kafka", 67, 19)
    text_row("Solicitud, respuesta y verificaciones", 95, 17)
    for index, line in enumerate(lines):
        text_row(line, 130 + index * 27, kind="stdout-line")
    footer_y = 160 + len(lines) * 27
    text_row("Fuente: " + SOURCE, footer_y, 15)
    text_row("SHA-256 stdout: " + sources["04-postman-stdout.txt"], footer_y + 27, 15)
    image = destination / "01-postman-cli-salida-renderizada.svg"
    image.write_bytes(ET.tostring(root, encoding="utf-8", xml_declaration=True))
    # Solo verificamos nuestra serialización conocida; no analizamos XML externo.
    serialized = image.read_bytes().decode("utf-8")
    rows = re.findall(
        r'<text\b(?=[^>]*\bclass="stdout-line")[^>]*?(?:/>|>(.*?)</text>)',
        serialized,
    )
    rendered = [html.unescape(row) for row in rows]
    assert "\n".join(rendered) == stdout, "Texto incompleto o alterado"
    assert all(sha256(run_dir / name) == digest for name, digest in sources.items())
    try:
        commands = json.loads((run_dir / "commands.json").read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SystemExit(f"No se pudo leer el registro de ejecución: {error}") from error
    execution = next(record for record in commands if record["name"] == "04-postman")
    metadata = {
        "classification": "postman-cli-stdout-visualization",
        "sourceRepoRelativePath": SOURCE,
        "sourceSHA256": sources["04-postman-stdout.txt"],
        "imageFile": image.name, "imageSHA256": sha256(image),
        "generatedAtUTC": datetime.now(timezone.utc).isoformat(),
        "renderer": "Python standard library xml.etree.ElementTree; SVG text",
        "font": "DejaVu Sans Mono, monospace", "width": width, "height": height,
        "contentPolicy": "full stdout, including warnings; no trimming or reconstruction",
        "textRoundTripVerified": True, "sourceFilesUnchanged": sources,
        "recordedPostmanExecution": execution, "additionalHTTPRequests": 0,
        "provenance": "Salida original identificada mediante ruta y SHA-256.",
    }
    manifest = destination / "01-postman-cli-salida-renderizada.json"
    manifest.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"svg": str(image), "manifest": str(manifest),
                      "textRoundTripVerified": True, "sourceFilesUnchanged": len(sources),
                      "additionalHTTPRequests": 0}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
