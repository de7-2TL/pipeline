from hooks.yfinance_hook import YfinanceNewsHook

def test_ticker():
    # given
    expected_industry_key = "semiconductors"
    expected_sector_key = "technology"
    company = "AMD"
    target = YfinanceNewsHook(company)
    page_size = 5

    # when
    result = target.get_news(page_size)

    # then
    assert result['company_info'] is not None
    assert result['company_info']['sectorKey'] == expected_sector_key
    assert result['company_info']['industryKey'] == expected_industry_key
    assert len(result['news']) == page_size


def test_do_not_call_get_conn_directly():
    # given
    company = "AMD"
    target = YfinanceNewsHook(company)

    # when / then
    try:
        target.get_conn()
        assert False, "Expected an exception when calling get_conn directly"
    except Exception as e:
        assert str(e) != ""




