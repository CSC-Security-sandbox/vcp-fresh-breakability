#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Mermaid code blocks in Markdown to images."
    )
    parser.add_argument(
        "--input",
        default="doc",
        help=(
            "Markdown file or directory to scan (default: doc). "
            "If a file is provided, only that file is processed."
        ),
    )
    parser.add_argument(
        "--output",
        default="doc/images/mermaid",
        help="Output directory for rendered images (default: doc/images/mermaid).",
    )
    parser.add_argument(
        "--format",
        default="svg",
        help=(
            "Output image format(s). Single: png or svg. Multiple: comma-separated (e.g. png,svg)."
        ),
    )
    parser.add_argument(
        "--config",
        default="",
        help="Optional Mermaid CLI config file path.",
    )
    parser.add_argument(
        "--puppeteer-config",
        default="",
        help="Optional Puppeteer config file path (passed as -p to mmdc).",
    )
    parser.add_argument(
        "--use-npx",
        action="store_true",
        help="Use npx to run mermaid-cli even if mmdc is installed.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print each rendered output path.",
    )
    return parser.parse_args()


def find_markdown_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path] if input_path.suffix == ".md" else []
    return [p for p in input_path.rglob("*.md") if p.is_file()]


def extract_mermaid_blocks(markdown_text: str) -> list[str]:
    blocks: list[str] = []
    in_block = False
    current: list[str] = []

    for line in markdown_text.splitlines():
        stripped = line.strip()
        if not in_block and stripped.startswith("```mermaid"):
            in_block = True
            current = []
            continue
        if in_block and stripped == "```":
            blocks.append("\n".join(current).strip() + "\n")
            in_block = False
            current = []
            continue
        if in_block:
            current.append(line)

    return blocks


def mermaid_command(use_npx: bool) -> list[str]:
    if not use_npx:
        mmdc = shutil.which("mmdc")
        if mmdc:
            return [mmdc]
    return ["npx", "-y", "@mermaid-js/mermaid-cli"]


def render_block(
    mermaid_cli: list[str],
    block: str,
    output_path: Path,
    config_path: str,
    puppeteer_config_path: str = "",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".mmd", delete=False) as tmp:
        tmp.write(block)
        tmp_path = tmp.name

    try:
        cmd = mermaid_cli + ["-i", tmp_path, "-o", str(output_path)]
        if config_path:
            cmd.extend(["-c", config_path])
        if puppeteer_config_path:
            cmd.extend(["-p", puppeteer_config_path])
        subprocess.run(cmd, check=True)
    finally:
        os.unlink(tmp_path)


def main() -> int:
    args = parse_args()

    input_path = Path(args.input).resolve()
    output_root = Path(args.output).resolve()
    config_path = Path(args.config).resolve() if args.config else ""
    puppeteer_config_path = (
        Path(args.puppeteer_config).resolve() if args.puppeteer_config else ""
    )

    if not input_path.exists():
        print(f"Input path not found: {input_path}", file=sys.stderr)
        return 1

    formats = [f.strip().lower() for f in args.format.split(",") if f.strip()]
    valid = {"png", "svg"}
    invalid = set(formats) - valid
    if invalid:
        print(f"Invalid format(s): {invalid}. Use png and/or svg.", file=sys.stderr)
        return 1
    if not formats:
        formats = ["svg"]

    mermaid_cli = mermaid_command(args.use_npx)

    md_files = find_markdown_files(input_path)
    total_blocks = 0
    rendered = 0

    for md_file in md_files:
        blocks = extract_mermaid_blocks(md_file.read_text(encoding="utf-8"))
        if not blocks:
            continue

        if input_path.is_file():
            rel_without_ext = Path(md_file.stem)
        else:
            rel_without_ext = md_file.relative_to(input_path).with_suffix("")
        output_dir = output_root / rel_without_ext

        for index, block in enumerate(blocks, start=1):
            total_blocks += 1
            for fmt in formats:
                output_name = f"diagram-{index:02d}.{fmt}"
                output_path = output_dir / output_name
                render_block(
                    mermaid_cli,
                    block,
                    output_path,
                    str(config_path),
                    str(puppeteer_config_path),
                )
                rendered += 1
                if args.verbose:
                    print(output_path)

    print(f"Rendered {rendered} of {total_blocks} Mermaid blocks ({len(formats)} format(s)).")
    print(f"Output root: {output_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

