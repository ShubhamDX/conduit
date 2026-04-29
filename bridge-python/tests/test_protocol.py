import json

from conduit_bridge.protocol import decode_request, encode_event, encode_response


def test_request_parsing():
    line = '{"jsonrpc":"2.0","id":1,"method":"newSession","params":{"prompt":"hi"}}'
    request = decode_request(line)
    assert request.id == 1
    assert request.method == "newSession"
    assert request.params["prompt"] == "hi"


def test_response_encoding():
    encoded = encode_response(1, {"session_id": "s1"})
    payload = json.loads(encoded)
    assert payload["jsonrpc"] == "2.0"
    assert payload["id"] == 1
    assert payload["result"]["session_id"] == "s1"


def test_event_encoding():
    encoded = encode_event({"kind": "agent_message_delta", "delta": "hi"})
    payload = json.loads(encoded)
    assert payload["jsonrpc"] == "2.0"
    assert payload["method"] == "event"
    assert payload["params"]["kind"] == "agent_message_delta"
