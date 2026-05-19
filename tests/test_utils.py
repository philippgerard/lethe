from lethe.utils import normalize_user_visible_message


def test_normalize_user_visible_message_suppresses_ok():
    assert normalize_user_visible_message("ok") == ""


def test_normalize_user_visible_message_strips_wrappers_and_keeps_real_text():
    assert normalize_user_visible_message("<result>Disk space critically low</result>") == "Disk space critically low"
