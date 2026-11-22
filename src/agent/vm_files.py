"""Fixed read-only commands executed by the agent over guest SSH."""

import codecs
import fnmatch
import os
import re
import stat
import subprocess
import sys

ROOT = "/home/mistl/work"
MAX_SEARCH_BYTES = 1024 * 1024


def within_root(path):
    return path == ROOT or path.startswith(ROOT + "/")


def resolve(path):
    path = os.path.realpath(os.path.join(ROOT, path), strict=True)
    if not within_root(path):
        raise ValueError("path is outside the workspace root")
    return path


def open_text(path):
    # Check the opened file as well, in case a path changed after resolution.
    stream = os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb")
    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
        stream.close()
        raise ValueError("path is not a file")
    if not within_root(os.path.realpath(f"/proc/self/fd/{stream.fileno()}")):
        stream.close()
        raise ValueError("path is outside the workspace root")
    return stream


def ignored_paths(directory):
    try:
        result = subprocess.run(
            ["/usr/bin/git", "-c", "core.excludesFile=/dev/null", "ls-files",
             "-ci", "-oi", "--exclude-standard", "--directory", "-z", "--", "."],
            cwd=directory, capture_output=True, check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return set()
    return {os.path.join(directory, os.fsdecode(path).rstrip("/"))
            for path in result.stdout.split(b"\0") if path}


def human_size(size):
    if size < 1024:
        return f"{size} B"
    for exponent, unit in enumerate(("KiB", "MiB", "GiB"), 1):
        if size < 1024 ** (exponent + 1) or unit == "GiB":
            return f"{size / 1024 ** exponent:.1f} {unit}"


def list_dir(path):
    directory = resolve(path)
    if not os.path.isdir(directory):
        raise ValueError("path is not a directory")
    ignored = ignored_paths(directory)
    entries = []
    for name in os.listdir(directory):
        if name.lower() == ".git":
            continue
        entry = os.path.join(directory, name)
        try:
            metadata = os.stat(resolve(entry))
        except (OSError, ValueError):
            continue
        is_dir = stat.S_ISDIR(metadata.st_mode)
        label = os.path.relpath(entry, ROOT)
        detail = "/" if is_dir else "  " + human_size(metadata.st_size)
        marker = " (ignored)" if entry in ignored else ""
        entries.append((not is_dir, label, label + detail + marker))
    lines = [line for _, _, line in sorted(entries)[:500]]
    if len(entries) > 500:
        lines.append(f"… {len(entries) - 500} more entries")
    return "\n".join(lines) or "(empty directory)"


def under(path, directory):
    return path == directory or path.startswith(directory + "/")


def workspace_files(scope, include_ignored):
    ignored = set() if include_ignored else ignored_paths(ROOT)
    for directory, dirs, files in os.walk(ROOT):
        if not include_ignored and directory != ROOT and ".git" in dirs + files:
            ignored.update(ignored_paths(directory))
        def visit(name):
            path = os.path.join(directory, name)
            return (name.lower() != ".git" and path not in ignored
                    and (under(path, scope) or under(scope, path)))

        dirs[:] = sorted(filter(visit, dirs))
        for name in sorted(files):
            path = os.path.join(directory, name)
            if name.lower() == ".git" or path in ignored or not under(path, scope):
                continue
            try:
                target = resolve(path)
            except (OSError, ValueError):
                continue
            if os.path.isfile(target):
                yield os.path.relpath(path, ROOT), target


def file_glob(pattern, path):
    if "/" not in pattern:
        return fnmatch.fnmatchcase(path.rsplit("/", 1)[-1], pattern)
    parts = path.split("/")
    positions = {0}
    for term in pattern.split("/"):
        if not positions:
            return False
        if term == "**":
            positions = range(min(positions), len(parts) + 1)
        else:
            positions = {i + 1 for i in positions if i < len(parts)
                         and fnmatch.fnmatchcase(parts[i], term)}
    return len(parts) in positions


def matches_summary(total, lines, label, omitted_label):
    if total > len(lines):
        lines.append(f"… {total - len(lines)} more {omitted_label}")
    return f"{total} {label}\n" + "\n".join(lines)


def find_files(glob, include_ignored):
    paths = []
    total = 0
    for name, _ in workspace_files(ROOT, include_ignored == "true"):
        if file_glob(glob, name):
            total += 1
            if len(paths) < 300:
                paths.append(name)
    return matches_summary(total, sorted(paths), "files matched", "files")


def short_line(line):
    return line[:300].replace("\r", " ") + ("…" if len(line) > 300 else "")


def grep(path, pattern, glob, case_insensitive):
    regex = re.compile(pattern, re.IGNORECASE if case_insensitive == "true" else 0)
    lines = []
    total = 0
    for name, target in workspace_files(resolve(path), False):
        if glob and not file_glob(glob, name):
            continue
        with open_text(target) as stream:
            if os.fstat(stream.fileno()).st_size > MAX_SEARCH_BYTES:
                continue
            content = stream.read(MAX_SEARCH_BYTES + 1)
        if len(content) > MAX_SEARCH_BYTES or b"\0" in content[:8192]:
            continue
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            continue
        parts = text.split("\n")
        for number, line in enumerate(parts, 1):
            if number < len(parts):
                line = line.removesuffix("\r")
            elif not line:
                break
            if regex.search(line):
                total += 1
                if len(lines) < 200:
                    lines.append(f"{name}:{number}: {short_line(line)}")
    return matches_summary(total, lines, "matching lines", "matching lines")


def read_file(path, offset, limit):
    target = resolve(path)
    offset, limit = int(offset), int(limit)
    lines = []
    total = 0
    with open_text(target) as stream:
        # Validate and count even long lines without retaining their whole contents.
        decoder = codecs.getincrementaldecoder("utf-8")()
        prefix = ""
        partial = False
        while chunk := stream.readline(8192):
            if b"\0" in chunk:
                raise ValueError("cannot read binary file")
            newline = chunk.endswith(b"\n")
            text = decoder.decode(chunk, final=newline)
            prefix = (prefix + text)[:303]
            partial = not newline
            if newline:
                total += 1
                if offset <= total < offset + limit:
                    display = prefix.removesuffix("\n").removesuffix("\r") if len(prefix) < 303 else prefix
                    lines.append(f"{total}: {short_line(display)}")
                prefix = ""
        decoder.decode(b"", final=True)
        if partial:
            total += 1
            if offset <= total < offset + limit:
                lines.append(f"{total}: {short_line(prefix)}")
    end = min(total, offset + len(lines) - 1)
    result = f"{os.path.relpath(target, ROOT)}: {total} total lines\n" + "\n".join(lines)
    if end < total:
        result += f"\nRead more with read_file offset={end + 1} limit={limit}."
    else:
        result += "\n(end of file)"
    if len(result.encode("utf-8", errors="replace")) > 12 * 1024:
        result += "\nOutput truncated; use read_file with a smaller limit to read all selected lines."
    return result


try:
    if os.getcwd() != ROOT:
        raise ValueError("workspace root resolves outside /home/mistl/work")
    tool = {"list_dir": list_dir, "find_files": find_files, "grep": grep, "read_file": read_file}
    sys.stdout.reconfigure(errors="replace")
    sys.stdout.write(tool[sys.argv[1]](*sys.argv[2:]))
except (OSError, ValueError, re.error) as error:
    sys.exit(str(error))
