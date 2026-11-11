"""Execute copies, keep notebook outputs and binary artifacts outside the sources."""
from pathlib import Path
import os
import ast
import tempfile
import nbformat
from nbclient import NotebookClient
from vmforecast.settings import Settings

cfg = Settings.load()
with tempfile.TemporaryDirectory(prefix='notebook-smoke-', dir=cfg.data) as temp:
    # No live extraction can be enabled by a participant's previous notebook run.
    os.environ['VMFORECAST_DATA_DIR'] = str(Path(temp)/'data')
    os.environ['VMFORECAST_RUNS_DIR'] = str(Path(temp)/'runs')
    for path in sorted((cfg.root/'notebooks').glob('*.ipynb')):
        book = nbformat.read(path, as_version=4)
        if path.name.startswith('01_'):
            for cell in book.cells:
                if cell.cell_type != 'code':
                    continue
                tree = ast.parse(cell.source)
                for node in ast.walk(tree):
                    if isinstance(node, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id in {'CONNECT_GOOGLE', 'EXECUTE_QUERIES'}
                        for t in node.targets
                    ):
                        node.value = ast.Constant(False)
                cell.source = ast.unparse(ast.fix_missing_locations(tree))
        NotebookClient(book, timeout=600, kernel_name='python3',
                       resources={'metadata': {'path': str(path.parent)}}).execute()
        print(path.name, 'OK')
