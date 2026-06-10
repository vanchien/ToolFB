"""
Relay HTTP local → SOCKS5 có xác thực (user/pass).

Chromium/Playwright không hỗ trợ ``socks5`` + ``username``/``password`` trên
``launch_persistent_context``. Tool tự mở ``http://127.0.0.1:<port>`` không auth,
forward qua SOCKS5 upstream (PySocks).
"""

from __future__ import annotations

import selectors
import socket
import socketserver
import threading
from typing import Any

from loguru import logger

try:
    import socks  # PySocks
except ImportError:
    socks = None  # type: ignore[assignment]


def socks_proxy_needs_http_relay(proxy: dict[str, Any]) -> bool:
    """SOCKS có auth → relay HTTP local cho Playwright (SOCKS5/SOCKS4)."""
    from src.utils.proxy_check import proxy_needs_socks_http_relay

    return proxy_needs_socks_http_relay(proxy)


def _strip_socks_host(raw: str) -> tuple[str, int | None]:
    h = str(raw or "").strip()
    if h.lower().startswith("socks5://"):
        h = h[9:]
    if "@" in h:
        h = h.rsplit("@", 1)[-1]
    if ":" in h:
        host, _, port_s = h.rpartition(":")
        try:
            return host.strip(), int(port_s)
        except ValueError:
            return h.strip(), None
    return h.strip(), None


class Socks5HttpRelay:
    """HTTP proxy trên loopback, upstream SOCKS5 có auth."""

    def __init__(
        self,
        upstream_host: str,
        upstream_port: int,
        *,
        username: str = "",
        password: str = "",
    ) -> None:
        if socks is None:
            raise RuntimeError("Chưa cài PySocks — pip install PySocks")
        self._upstream_host = upstream_host
        self._upstream_port = int(upstream_port)
        self._username = username
        self._password = password
        self._server: socketserver.ThreadingTCPServer | None = None
        self._thread: threading.Thread | None = None
        self.local_port: int = 0
        self.local_url: str = ""

    @classmethod
    def from_proxy_config(cls, proxy: dict[str, Any]) -> Socks5HttpRelay:
        raw_host = str(proxy.get("host", "")).strip()
        try:
            port = int(proxy.get("port", 0))
        except (TypeError, ValueError):
            port = 0
        embedded_host, embedded_port = _strip_socks_host(raw_host)
        if embedded_port and embedded_port > 0:
            port = embedded_port
        host = embedded_host
        if not host or port <= 0:
            raise ValueError("SOCKS5 relay: thiếu host/port.")
        user = str(proxy.get("user", "") or proxy.get("username", "")).strip()
        pwd = str(proxy.get("pass", "") or proxy.get("password", "")).strip()
        return cls(host, port, username=user, password=pwd)

    def start(self) -> str:
        if self._server is not None:
            return self.local_url
        upstream = self

        class _Handler(socketserver.BaseRequestHandler):
            def handle(self) -> None:
                upstream._handle_client(self.request)

        self._server = socketserver.ThreadingTCPServer(
            ("127.0.0.1", 0),
            _Handler,
        )
        self._server.daemon_threads = True
        self._server.allow_reuse_address = True
        self.local_port = int(self._server.server_address[1])
        self.local_url = f"http://127.0.0.1:{self.local_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(
            "SOCKS5 relay HTTP local {} → socks5://{}:{} (auth={})",
            self.local_url,
            self._upstream_host,
            self._upstream_port,
            bool(self._username),
        )
        return self.local_url

    def stop(self) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
            self._server.server_close()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Đóng SOCKS5 relay: {}", exc)
        self._server = None
        self._thread = None
        logger.info("Đã dừng SOCKS5 HTTP relay (port {}).", self.local_port)

    def _open_socks(self, dest_host: str, dest_port: int) -> socket.socket:
        sock = socks.socksocket()  # type: ignore[union-attr]
        sock.set_proxy(
            socks.SOCKS5,  # type: ignore[union-attr]
            self._upstream_host,
            self._upstream_port,
            username=self._username or None,
            password=self._password or None,
        )
        sock.settimeout(90.0)
        sock.connect((dest_host, dest_port))
        sock.settimeout(None)
        return sock

    @staticmethod
    def _relay_bidirectional(a: socket.socket, b: socket.socket) -> None:
        sel = selectors.DefaultSelector()
        try:
            sel.register(a, selectors.EVENT_READ)
            sel.register(b, selectors.EVENT_READ)
            while True:
                for key, _ in sel.select(timeout=120.0):
                    src = key.fileobj
                    if src is None:
                        continue
                    dst = b if src is a else a
                    data = src.recv(65536)
                    if not data:
                        return
                    dst.sendall(data)
        finally:
            sel.close()

    def _handle_client(self, client: socket.socket) -> None:
        client.settimeout(60.0)
        try:
            buf = b""
            while b"\r\n\r\n" not in buf and len(buf) < 65536:
                chunk = client.recv(4096)
                if not chunk:
                    return
                buf += chunk
            header_end = buf.find(b"\r\n\r\n")
            if header_end < 0:
                return
            header = buf[:header_end].decode("latin-1", errors="replace")
            rest = buf[header_end + 4 :]
            lines = header.split("\r\n")
            if not lines:
                return
            parts = lines[0].split()
            if len(parts) < 2:
                return
            method, target = parts[0].upper(), parts[1]
            if method == "CONNECT":
                if ":" in target:
                    host, port_s = target.rsplit(":", 1)
                    port = int(port_s)
                else:
                    host, port = target, 443
                remote = self._open_socks(host, port)
                client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                if rest:
                    remote.sendall(rest)
                self._relay_bidirectional(client, remote)
                remote.close()
            else:
                # HTTP tuyệt đối — ít gặp với Playwright nhưng hỗ trợ cơ bản
                if target.startswith("http://"):
                    from urllib.parse import urlparse

                    parsed = urlparse(target)
                    host = parsed.hostname or ""
                    port = parsed.port or 80
                    path = parsed.path or "/"
                    if parsed.query:
                        path += "?" + parsed.query
                    remote = self._open_socks(host, port)
                    req_line = f"{method} {path} HTTP/1.1\r\n"
                    hop = [ln for ln in lines[1:] if not ln.lower().startswith("proxy-")]
                    payload = (req_line + "\r\n".join(hop) + "\r\n\r\n").encode("latin-1") + rest
                    remote.sendall(payload)
                    self._relay_bidirectional(client, remote)
                    remote.close()
        except Exception as exc:  # noqa: BLE001
            logger.debug("SOCKS5 relay client: {}", exc)
        finally:
            try:
                client.close()
            except OSError:
                pass


