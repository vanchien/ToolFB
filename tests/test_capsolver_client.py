"""Tests CapSolver client (mock HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.capsolver_client import (
    CAPSOLVER_FB_PROXYLESS_WEBSITE_URL,
    CapSolverError,
    proxy_config_to_capsolver_task_fields,
    proxy_url_to_capsolver_format,
    proxy_url_to_capsolver_task_fields,
    solve_recaptcha_v2,
)


def test_proxy_url_parsed_to_structured_fields_not_legacy_string() -> None:
    fields = proxy_url_to_capsolver_task_fields("socks5://admin:secret@160.30.191.116:20608")
    assert fields == {
        "proxyType": "socks5",
        "proxyAddress": "160.30.191.116",
        "proxyPort": 20608,
        "proxyLogin": "admin",
        "proxyPassword": "secret",
    }


def test_proxy_url_to_capsolver_format() -> None:
    assert (
        proxy_url_to_capsolver_format("http://user:pass@203.175.96.175:25308")
        == "http:203.175.96.175:25308:user:pass"
    )
    assert (
        proxy_url_to_capsolver_format("socks5://admin:secret@160.30.191.116:20608")
        == "socks5:160.30.191.116:20608:admin:secret"
    )


def test_proxy_config_port_coerced_from_string() -> None:
    fields = proxy_config_to_capsolver_task_fields(
        {
            "host": "160.30.191.116",
            "port": "20608",
            "user": "u",
            "pass": "p",
            "scheme_hint": "socks5",
        }
    )
    assert fields["proxyPort"] == 20608
    assert isinstance(fields["proxyPort"], int)


def test_proxy_config_to_capsolver_structured_fields() -> None:
    fields = proxy_config_to_capsolver_task_fields(
        {
            "host": "socks5://160.30.191.116",
            "port": 20608,
            "user": "admin254",
            "pass": "admin254",
            "scheme_hint": "socks5",
        }
    )
    assert fields == {
        "proxyType": "socks5",
        "proxyAddress": "160.30.191.116",
        "proxyPort": 20608,
        "proxyLogin": "admin254",
        "proxyPassword": "admin254",
    }


def test_solve_recaptcha_v2_uses_structured_proxy() -> None:
    create_resp = MagicMock()
    create_resp.json.return_value = {"errorId": 0, "taskId": "task-px"}
    create_resp.raise_for_status = MagicMock()
    result_resp = MagicMock()
    result_resp.json.return_value = {
        "errorId": 0,
        "status": "ready",
        "solution": {"gRecaptchaResponse": "tok"},
    }
    result_resp.raise_for_status = MagicMock()
    px = {
        "host": "socks5://160.30.191.116",
        "port": 20608,
        "user": "u",
        "pass": "p",
        "scheme_hint": "socks5",
    }
    with patch("src.services.capsolver_client.requests.post", side_effect=[create_resp, result_resp]) as mp:
        with patch("src.services.capsolver_client.time.sleep"):
            solve_recaptcha_v2(
                website_url="https://www.facebook.com/",
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="test-key",
                proxy_config=px,
                is_enterprise=True,
            )
        task = mp.call_args_list[0].kwargs["json"]["task"]
        assert task["type"] == "ReCaptchaV2EnterpriseTask"
        assert task["proxyType"] == "socks5"
        assert task["proxyAddress"] == "160.30.191.116"
        assert task["proxyPort"] == 20608
        assert isinstance(task["proxyPort"], int)
        assert "proxy" not in task
        assert task["websiteURL"] == "https://www.facebook.com/"


def test_solve_recaptcha_v2_success() -> None:
    create_resp = MagicMock()
    create_resp.json.return_value = {"errorId": 0, "taskId": "task-1"}
    create_resp.raise_for_status = MagicMock()

    result_resp = MagicMock()
    result_resp.json.return_value = {
        "errorId": 0,
        "status": "ready",
        "solution": {"gRecaptchaResponse": "03AGdBq-test-token"},
    }
    result_resp.raise_for_status = MagicMock()

    with patch("src.services.capsolver_client.requests.post", side_effect=[create_resp, result_resp]):
        with patch("src.services.capsolver_client.time.sleep"):
            sol = solve_recaptcha_v2(
                website_url="https://www.facebook.com/checkpoint",
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="test-key",
            )
    assert sol["gRecaptchaResponse"] == "03AGdBq-test-token"


def test_solve_recaptcha_v2_create_error() -> None:
    bad = MagicMock()
    bad.json.return_value = {"errorId": 1, "errorCode": "ERROR_KEY", "errorDescription": "bad key"}
    bad.raise_for_status = MagicMock()
    with patch("src.services.capsolver_client.requests.post", return_value=bad):
        with pytest.raises(CapSolverError, match="createTask"):
            solve_recaptcha_v2(
                website_url="https://example.com",
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="x",
            )


def test_capsolver_website_url_for_task_strips_two_step_on_proxyless() -> None:
    from src.services.capsolver_client import capsolver_website_url_for_task

    long_url = (
        "https://www.facebook.com/two_step_verification/authentication/"
        "?encrypted_context=AWSrSvuHWufaygooCat7nrUftM7xN9LYceE1HzLqR51ApDKb25mcaemv4bGgNSOqoT_MbZnzoLpjeIHoUdDs8R5hmLBVgKYoGlY"
    )
    assert capsolver_website_url_for_task(long_url, proxyless=True).endswith("/authentication")
    assert "encrypted_context" not in capsolver_website_url_for_task(long_url, proxyless=True)
    assert "encrypted_context" not in capsolver_website_url_for_task(long_url, proxyless=False)


def test_solve_proxyless_uses_canonical_facebook_domain_when_requested() -> None:
    create_resp = MagicMock()
    create_resp.json.return_value = {"errorId": 0, "taskId": "task-pl"}
    create_resp.raise_for_status = MagicMock()
    result_resp = MagicMock()
    result_resp.json.return_value = {
        "errorId": 0,
        "status": "ready",
        "solution": {"gRecaptchaResponse": "tok"},
    }
    result_resp.raise_for_status = MagicMock()
    with patch("src.services.capsolver_client.requests.post", side_effect=[create_resp, result_resp]) as mp:
        with patch("src.services.capsolver_client.time.sleep"):
            solve_recaptcha_v2(
                website_url=CAPSOLVER_FB_PROXYLESS_WEBSITE_URL,
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="test-key",
                is_enterprise=True,
            )
    task = mp.call_args_list[0].kwargs["json"]["task"]
    assert task["websiteURL"] == CAPSOLVER_FB_PROXYLESS_WEBSITE_URL
    assert task["type"] == "ReCaptchaV2EnterpriseTaskProxyLess"
    assert "proxy" not in task
    assert "proxyAddress" not in task


def test_solve_recaptcha_v2_enterprise_task_type() -> None:
    create_resp = MagicMock()
    create_resp.json.return_value = {"errorId": 0, "taskId": "task-ent"}
    create_resp.raise_for_status = MagicMock()
    result_resp = MagicMock()
    result_resp.json.return_value = {
        "errorId": 0,
        "status": "ready",
        "solution": {"gRecaptchaResponse": "ent-token"},
    }
    result_resp.raise_for_status = MagicMock()
    with patch("src.services.capsolver_client.requests.post", side_effect=[create_resp, result_resp]) as mocked_post:
        with patch("src.services.capsolver_client.time.sleep"):
            solve_recaptcha_v2(
                website_url="https://www.facebook.com/two_step_verification/",
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="test-key",
                is_enterprise=True,
                recaptcha_data_s_value="s-token",
                page_action="verify",
                api_domain="www.google.com",
            )
    create_payload = mocked_post.call_args_list[0].kwargs.get("json") or {}
    assert create_payload["task"]["type"] == "ReCaptchaV2EnterpriseTaskProxyLess"
    assert create_payload["task"]["websiteURL"] == "https://www.facebook.com/two_step_verification"
    assert create_payload["task"]["enterprisePayload"] == {"s": "s-token"}
    assert create_payload["task"]["pageAction"] == "verify"
    assert create_payload["task"]["apiDomain"] == "www.google.com"


def test_solve_recaptcha_v2_passes_user_agent() -> None:
    create_resp = MagicMock()
    create_resp.json.return_value = {"errorId": 0, "taskId": "task-ua"}
    create_resp.raise_for_status = MagicMock()
    result_resp = MagicMock()
    result_resp.json.return_value = {
        "errorId": 0,
        "status": "ready",
        "solution": {"gRecaptchaResponse": "tok"},
    }
    result_resp.raise_for_status = MagicMock()
    ua = "Mozilla/5.0 (Windows NT 10.0; rv:128.0) Gecko/20100101 Firefox/128.0"
    with patch("src.services.capsolver_client.requests.post", side_effect=[create_resp, result_resp]) as mp:
        with patch("src.services.capsolver_client.time.sleep"):
            solve_recaptcha_v2(
                website_url="https://www.facebook.com/",
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="test-key",
                is_enterprise=True,
                recaptcha_data_s_value="long-s-value",
                user_agent=ua,
            )
        body = mp.call_args_list[0].kwargs["json"]["task"]
        assert body["userAgent"] == ua
        assert body["enterprisePayload"] == {"s": "long-s-value"}


def test_solve_recaptcha_v2_http_400_contains_body() -> None:
    bad = MagicMock()
    bad.status_code = 400
    bad.text = '{"errorId":1,"errorCode":"ERROR_PROXY"}'
    with patch("src.services.capsolver_client.requests.post", return_value=bad):
        with pytest.raises(CapSolverError, match="createTask HTTP 400"):
            solve_recaptcha_v2(
                website_url="https://example.com",
                website_key="6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-",
                api_key="x",
            )
