from src.live.cooldown_resolver import attach_thesis_trace_fields as _attach_thesis_trace_fields


def test_attach_thesis_trace_fields_adds_conviction_payload() -> None:
    trace = {"signal": "long"}
    snapshot = {
        "conviction": 77.5,
        "status": "decaying",
        "time_decay": 4.5,
        "zone_rejection": 0.0,
        "volume_fade": 15.0,
    }

    out = _attach_thesis_trace_fields(trace, snapshot)

    assert out["thesis_conviction"] == 77.5
    assert out["thesis_status"] == "decaying"
    assert out["thesis_decay"]["time_decay"] == 4.5
