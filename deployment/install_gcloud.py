"""Install a pinned official Google Cloud CLI without system Python or sudo.

Release/hash pair verified against the official versioned HTTPS archive, 2026-11-11.
"""
from pathlib import Path
import hashlib
import os
import platform
import shutil
import tarfile
import tempfile
import urllib.request

VERSION = '593.0.0'
ARCHIVES = {
    'x86_64': ('x86_64', '42c32b7553c712f71fcf6f124546c0519437f53755f47f9bd0c957998cd6b826'),
    'aarch64': ('arm', 'f40acaca49c9774bca035c96dcf7350a13c09007e906cb4832e91dc36ce11497'),
}


def target(cfg):
    return cfg.state/'tools'/('gcloud-'+VERSION)/'google-cloud-sdk/bin/gcloud'


def install(cfg):
    if platform.system() != 'Linux' or platform.machine() not in ARCHIVES:
        raise ValueError('Google Cloud CLI installer supports Linux x86_64/aarch64')
    binary = target(cfg)
    if binary.is_file():
        print('Google Cloud CLI already installed:', binary)
        return binary
    destination = binary.parents[2]
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.exists():
        raise ValueError('Incomplete CLI target already exists; choose another state directory; it was not overwritten')
    if shutil.disk_usage(destination.parent).free < 1024**3:
        raise ValueError('Google Cloud CLI installer requires at least 1 GiB free')
    architecture, expected = ARCHIVES[platform.machine()]
    url = f'https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-{VERSION}-linux-{architecture}.tar.gz'
    with tempfile.TemporaryDirectory(prefix='gcloud-install-', dir=destination.parent) as temp:
        archive = Path(temp)/'sdk.tar.gz'
        digest = hashlib.sha256()
        try:
            with urllib.request.urlopen(url, timeout=30) as response, archive.open('wb') as out:
                while chunk := response.read(1024*1024):
                    out.write(chunk)
                    digest.update(chunk)
        except OSError:
            raise ValueError('Could not download Google Cloud CLI; check network and retry sh lab install-gcloud') from None
        if digest.hexdigest() != expected:
            raise ValueError('Google Cloud CLI SHA-256 mismatch; no archive was installed')
        unpacked = Path(temp)/'unpacked'
        unpacked.mkdir()
        with tarfile.open(archive, 'r:gz') as stream:
            for member in stream.getmembers():
                if Path(member.name).parts[:1] != ('google-cloud-sdk',):
                    raise ValueError('Unexpected archive layout; no installation performed')
            stream.extractall(unpacked, filter='data')
        if not (unpacked/'google-cloud-sdk/bin/gcloud').is_file():
            raise ValueError('CLI executable missing from archive')
        os.replace(unpacked, destination)
    print('Google Cloud CLI installed:', binary)
    return binary
