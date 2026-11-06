"""Replies with big inline images must still fit Close's 1 MB body_html limit (cmarky@biotekrx.com, 2026-10-12)."""

from temporal.activities.instantly.webhook_reply_received import (
    CLOSE_BODY_HTML_MAX,
    fit_body_html_for_close,
)


def test_small_html_unchanged():
    html = '<p>Hi</p><img src="data:image/png;base64,AAAA">'
    assert fit_body_html_for_close(html) == html


def test_inline_images_stripped_when_too_long():
    img = "data:image/png;base64," + "A" * CLOSE_BODY_HTML_MAX
    out = fit_body_html_for_close(f'<p>Q2 of 2027</p><img src="{img}">')
    assert out == '<p>Q2 of 2027</p><img src="">'


def test_still_too_long_falls_back_to_text_only():
    assert fit_body_html_for_close("<p>" + "x" * CLOSE_BODY_HTML_MAX + "</p>") == ""
