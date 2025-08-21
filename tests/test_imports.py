def test_imports():
    # Minimal smoke test: ensure our modules import without executing logic
    __import__("src.peek".replace("/", "."))
    __import__("src.explore_311".replace("/", "."))
    __import__("src.fetch".replace("/", "."))