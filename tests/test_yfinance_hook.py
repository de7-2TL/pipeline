from hooks.yfinance_hook import YfinanceNewsHook


def test_ticker():
    # given
    company = ["AMD", "AAPL"]
    target = YfinanceNewsHook(company)

    # when
    result = target.get_news()

    # then
    for v in result:
        assert v["company_key"] in company


def test_do_not_call_get_conn_directly():
    # given
    company = ["AMD", "AAPL"]
    target = YfinanceNewsHook(company)

    # when / then
    try:
        target.get_conn()
        assert False, "Expected an exception when calling get_conn directly"
    except Exception as e:
        assert str(e) != ""
