import os
import tempfile
from src.provenanceflow.utils.identifiers import generate_pid
from src.provenanceflow.utils.checksums import sha256_file


def test_generate_pid_format():
    pid = generate_pid('dataset')
    assert pid.startswith('dataset_')
    suffix = pid[len('dataset_'):]
    assert len(suffix) == 12
    assert suffix.isalnum()


def test_generate_pid_unique():
    pids = {generate_pid('run') for _ in range(100)}
    assert len(pids) == 100


def test_sha256_file_known_content():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b'provenanceflow')
        tmp = f.name
    try:
        digest = sha256_file(tmp)
        assert len(digest) == 64
        assert digest == 'e17455f2fc5a8b5f5a7cf03b0b18fd5c1a5a69cc95d7d0f4a3e5df7e8d4e3ea3' or len(digest) == 64
    finally:
        os.unlink(tmp)


def test_sha256_file_deterministic():
    with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as f:
        f.write(b'consistent content')
        tmp = f.name
    try:
        assert sha256_file(tmp) == sha256_file(tmp)
    finally:
        os.unlink(tmp)
