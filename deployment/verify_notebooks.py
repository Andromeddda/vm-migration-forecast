"""Execute copies, keep notebook outputs and binary artifacts outside the sources."""
from pathlib import Path
import ast
import copy
import os
import tempfile
import nbformat
from nbclient import NotebookClient
from vmforecast.settings import Settings

def synthetic_copy(book):
    """Ignore a participant's saved dataset toggle; never edit the source notebook."""
    book = copy.deepcopy(book)
    for cell in book.cells:
        if cell.cell_type != 'code' or 'USE_PREPARED' not in cell.source:
            continue
        tree = ast.parse(cell.source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == 'USE_PREPARED' for t in node.targets
            ):
                node.value = ast.Constant(False)
        cell.source = ast.unparse(ast.fix_missing_locations(tree))
    return book


def main():
    cfg = Settings.load()
    with tempfile.TemporaryDirectory(prefix='notebook-smoke-', dir=cfg.data) as temp:
        # Run only local experiments with isolated data/results.
        os.environ['VMFORECAST_DATA_DIR'] = str(Path(temp)/'data')
        os.environ['VMFORECAST_RUNS_DIR'] = str(Path(temp)/'runs')
        for path in sorted((cfg.root/'notebooks').glob('*.ipynb')):
            book = synthetic_copy(nbformat.read(path, as_version=4))
            NotebookClient(book, timeout=600, kernel_name='python3',
                           resources={'metadata': {'path': str(path.parent)}}).execute()
            print(path.name, 'OK')


if __name__ == '__main__':
    main()
