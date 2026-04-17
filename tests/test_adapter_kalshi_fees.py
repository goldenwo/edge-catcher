from edge_catcher.adapters.kalshi.fees import STANDARD_FEE, INDEX_FEE


def test_kalshi_fees_are_exported():
	assert STANDARD_FEE.id == "standard"
	assert INDEX_FEE.id == "index"


def test_kalshi_standard_fee_formula():
	# 7% taker fee at price=50c, 1 contract: ceil(0.07 * 0.5 * 0.5 * 100) = 2
	assert STANDARD_FEE.calculate(price=50, size=1) == 2
