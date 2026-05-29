from src.services.page_insights_parse import metrics_from_body_text, merge_metrics
from src.utils.page_insights_format import format_metric, parse_metric_number


def test_parse_metric_number() -> None:
    assert parse_metric_number("12,345") == 12345
    assert parse_metric_number("1.2K") == 1200
    assert parse_metric_number("3M") == 3_000_000
    assert parse_metric_number(None) is None


def test_format_metric() -> None:
    assert format_metric(15_000) == "15K"
    assert format_metric(None) == "—"


def test_metrics_from_body_text() -> None:
    body = "Total followers\n12,340\nContent views\n98,765"
    f, v = metrics_from_body_text(body)
    assert f == 12340
    assert v == 98765


def test_merge_metrics() -> None:
    f, v = merge_metrics({"followers": 10}, {"views": 20})
    assert f == 10 and v == 20
