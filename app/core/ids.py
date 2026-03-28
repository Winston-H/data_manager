import os
import secrets
import threading
import time


def new_trace_id() -> str:
    return secrets.token_hex(8)


_record_id_lock = threading.Lock()
_record_id_last_ms = 0
_record_id_seq = 0
_record_id_pid = os.getpid() & 0x3FF
_record_id_epoch_ms = 1_735_660_800_000


def new_record_id() -> int:
    global _record_id_last_ms, _record_id_seq

    now_ms = int(time.time() * 1000)
    with _record_id_lock:
        if now_ms < _record_id_last_ms:
            now_ms = _record_id_last_ms
        if now_ms == _record_id_last_ms:
            _record_id_seq = (_record_id_seq + 1) & 0xFFF
            if _record_id_seq == 0:
                while now_ms <= _record_id_last_ms:
                    now_ms = int(time.time() * 1000)
        else:
            _record_id_seq = 0
        _record_id_last_ms = now_ms
        return ((now_ms - _record_id_epoch_ms) << 22) | (_record_id_pid << 12) | _record_id_seq
