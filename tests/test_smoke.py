from npt.settings import Settings


def test_settings_dirs(tmp_path):
    s = Settings(data_dir=tmp_path / "data")
    s.ensure_dirs()
    assert s.bronze_dir.exists()
    assert s.silver_dir.exists()
    assert s.gold_dir.exists()