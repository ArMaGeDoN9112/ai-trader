"""Run the real CLI with external I/O denied, including without API-key env vars."""

from pathlib import Path
import runpy
import sys


def deny_external_io(event: str, args: tuple[object, ...]) -> None:
    if event.startswith(('socket.', 'subprocess.', 'os.exec', 'os.spawn')) or event == 'os.system':
        raise RuntimeError(f'FORBIDDEN_EXTERNAL_CALL: {event}')


sys.addaudithook(deny_external_io)
if sys.argv[1:2] == ['--probe']:
    probe = sys.argv[2]
    if probe == 'socket':
        import socket
        socket.create_connection(('127.0.0.1', 9))
    elif probe == 'http':
        import urllib.request
        urllib.request.urlopen('https://example.invalid', timeout=1)
    elif probe == 'subprocess':
        import subprocess
        subprocess.run([sys.executable, '-c', 'pass'], check=True)
    elif probe == 'shell':
        import os
        os.system('echo unexpected')
else:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    sys.argv[0] = 'news_futures'
    runpy.run_module('news_futures', run_name='__main__')
