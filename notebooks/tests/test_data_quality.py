def test_positive_amount(df):
    assert df.filter(df.amount <= 0).count() == 0, "Negative or zero spend found"

def test_valid_suppliers(df):
    assert df.filter(df.supplier_id.isNull()).count() == 0, "Null supplier IDs found"
