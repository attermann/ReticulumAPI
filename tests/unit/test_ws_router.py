import pytest

from rnsapi.ws.router import WSRouter


class FakeConn:
    def __init__(self):
        self.id = "fake"
        self.sent: list[dict] = []

    async def send_json(self, data: dict):
        self.sent.append(data)


@pytest.mark.asyncio
async def test_dispatch_calls_registered_handler():
    router = WSRouter()
    heard = []

    async def h(conn, msg):
        heard.append(msg)

    router.register("ping", h)
    conn = FakeConn()
    await router.dispatch(conn, {"type": "ping", "id": "1"})
    assert heard == [{"type": "ping", "id": "1"}]


@pytest.mark.asyncio
async def test_dispatch_unknown_type_sends_error():
    router = WSRouter()
    conn = FakeConn()
    await router.dispatch(conn, {"type": "does.not.exist", "id": "x"})
    assert len(conn.sent) == 1
    assert conn.sent[0]["error"] == "unknown_type"
    assert conn.sent[0]["requested_type"] == "does.not.exist"
    assert conn.sent[0]["id"] == "x"


@pytest.mark.asyncio
async def test_dispatch_missing_type_sends_error():
    router = WSRouter()
    conn = FakeConn()
    await router.dispatch(conn, {"id": "1"})
    assert conn.sent[0]["error"] == "missing_type"


def test_double_register_raises():
    router = WSRouter()

    async def h(conn, msg):
        pass

    router.register("x", h)
    with pytest.raises(ValueError):
        router.register("x", h)


@pytest.mark.asyncio
async def test_handler_exception_produces_internal_error():
    router = WSRouter()

    async def h(conn, msg):
        raise RuntimeError("boom")

    router.register("x", h)
    conn = FakeConn()
    await router.dispatch(conn, {"type": "x", "id": "y"})
    assert conn.sent[0]["error"] == "internal"
    assert conn.sent[0]["id"] == "y"
