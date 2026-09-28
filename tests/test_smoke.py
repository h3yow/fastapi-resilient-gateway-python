"""骨架冒烟测试：包可以导入、配置可以加载。"""


def test_import_gateway() -> None:
    import gateway

    assert gateway.create_app is not None
