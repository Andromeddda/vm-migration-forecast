"""Bounded source hygiene checks. No credentials or suspicious file contents are printed."""
from pathlib import Path
import json
import re

TEXT_SUFFIXES = {'.py', '.toml', '.lock', '.md', '.yaml', '.yml', '.json', '.ipynb', '.example', '.sh', ''}


def clean(root):
    for p in (root/'notebooks').glob('*.ipynb'):
        doc = json.loads(p.read_text())
        doc['metadata'].pop('widgets', None)
        for cell in doc['cells']:
            if cell['cell_type'] == 'code':
                cell['outputs'] = []
                cell['execution_count'] = None
            cell['metadata'] = {}
        p.write_text(json.dumps(doc, indent=1, ensure_ascii=False)+'\n')
    print('Notebook outputs and execution metadata cleared.')


def audit(root):
    failures = []
    forbidden = [r'google[.]colab', r'/con' + r'tent/', r'/ho' + r'me/[^/\s]+/', r'/Us' + r'ers/[^/\s]+/',
                 r'-----BEGIN [A-Z ]*PRIVATE KEY-----', r'ya29\.[A-Za-z0-9_-]{15,}',
                 r'1//[A-Za-z0-9_-]{20,}']
    for p in root.rglob('*'):
        if not p.is_file():
            continue
        relative = p.relative_to(root)
        if any(part in {'.git', '__pycache__', '.pytest_cache'} for part in relative.parts):
            failures.append(str(relative)+': generated directory')
            continue
        if p.name.startswith('.env') and p.name != '.env.example':
            failures.append(str(relative)+': participant configuration')
        try:
            text = p.read_text()
            if '\x00' in text or (p.suffix not in TEXT_SUFFIXES and p.name not in {'.gitignore', '.python-version'}):
                failures.append(str(relative)+': non-source file')
        except (UnicodeError, OSError):
            failures.append(str(relative)+': not readable UTF-8 text')
            continue
        if any(re.search(pattern, text) for pattern in forbidden):
            failures.append(str(relative)+': private/runtime data or unsupported runtime dependency')
        if p.suffix == '.ipynb':
            doc = json.loads(text)
            if any(c.get('outputs') or c.get('execution_count') is not None for c in doc['cells'] if c['cell_type'] == 'code'):
                failures.append(str(relative)+': saved execution outputs')
    if failures:
        raise ValueError('Source audit failed:\n'+'\n'.join(failures))
    print('Source audit OK: UTF-8 text, notebooks without saved output, no known credential patterns.')
