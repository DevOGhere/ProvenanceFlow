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
        assert digest == '324d7d5be8a556ce498e7a69782b19f17d93d43c6420f4dabc7f69cd7e183f0a'
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


def test_sha256_file_nonexistent_raises_file_not_found_error():
    import pytest
    with pytest.raises(FileNotFoundError):
        sha256_file('/nonexistent/path/that/does/not/exist.csv')
