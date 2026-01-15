"""Mutual Fund equity/debt classification logic."""

from .models import AssetClass


# Equity fund keywords (>65% equity exposure)
EQUITY_KEYWORDS = [
    'EQUITY',
    'BLUECHIP',
    'BLUE CHIP',
    'LARGE CAP',
    'LARGECAP',
    'MID CAP',
    'MIDCAP',
    'SMALL CAP',
    'SMALLCAP',
    'MULTI CAP',
    'MULTICAP',
    'FLEXI CAP',
    'FLEXICAP',
    'FOCUSED',
    'SECTORAL',
    'THEMATIC',
    'INDEX',
    'NIFTY',
    'SENSEX',
    'TOP 100',  # Large cap indicator
    'TOP 200',
    'CONSUMPTION',
    'PHARMA',
    'BANKING',
    'FINANCIAL SERVICES',
    'TECHNOLOGY',
    'INFRASTRUCTURE',
    'PSU',
    'VALUE',
    'MOMENTUM',
    'ELSS',  # Equity-Linked Savings Scheme
    'TAX SAVER',
    'GROWTH FUND',  # Generic equity fund names
    'VISION FUND',
    'OPPORTUNITIES',
    'CONTRA',
    'DIVIDEND YIELD',
    'DIVERSIFIED',
]

# Debt fund keywords (<65% equity exposure)
DEBT_KEYWORDS = [
    'DEBT',
    'BOND',
    'GILT',
    'LIQUID',
    'MONEY MARKET',
    'ULTRA SHORT',
    'LOW DURATION',
    'SHORT DURATION',
    'MEDIUM DURATION',
    'LONG DURATION',
    'DYNAMIC BOND',
    'CORPORATE BOND',
    'CREDIT RISK',
    'BANKING & PSU',
    'OVERNIGHT',
    'TREASURY',
]

# Hybrid fund keywords (both equity and debt)
HYBRID_KEYWORDS = [
    'HYBRID',
    'BALANCED',
    'AGGRESSIVE HYBRID',
    'CONSERVATIVE HYBRID',
    'EQUITY SAVINGS',
    'ARBITRAGE',
    'MULTI ASSET',
]


def classify_scheme(scheme_name: str) -> AssetClass:
    """
    Classify a mutual fund scheme as EQUITY, DEBT, HYBRID, or OTHER.

    Classification rules:
    1. Check for hybrid keywords first (they may contain both equity/debt keywords)
    2. Check for debt fund keywords (debt keywords are more specific)
    3. Check for equity fund keywords
    4. Default to OTHER if unclear

    Args:
        scheme_name: Scheme name from CAMS CAS

    Returns:
        AssetClass enum value

    Examples:
        >>> classify_scheme("SBI Bluechip Fund Direct Growth")
        AssetClass.EQUITY

        >>> classify_scheme("HDFC Corporate Bond Fund")
        AssetClass.DEBT

        >>> classify_scheme("ICICI Prudential Balanced Advantage Fund")
        AssetClass.HYBRID
    """
    if not scheme_name:
        return AssetClass.OTHER

    scheme_upper = scheme_name.upper()

    # Check hybrid first (as they contain both equity and debt keywords)
    for keyword in HYBRID_KEYWORDS:
        if keyword in scheme_upper:
            return AssetClass.HYBRID

    # Check debt keywords BEFORE equity (debt keywords are more specific)
    # This prevents "HDFC Corporate Bond Fund Growth" from being classified as EQUITY
    for keyword in DEBT_KEYWORDS:
        if keyword in scheme_upper:
            return AssetClass.DEBT

    # Check equity keywords
    for keyword in EQUITY_KEYWORDS:
        if keyword in scheme_upper:
            return AssetClass.EQUITY

    # Default to OTHER if no match
    return AssetClass.OTHER


def get_holding_period_threshold(asset_class: AssetClass) -> int:
    """
    Get the holding period threshold (in days) for LTCG classification.

    Args:
        asset_class: EQUITY or DEBT

    Returns:
        Number of days for LTCG threshold

    Examples:
        >>> get_holding_period_threshold(AssetClass.EQUITY)
        365

        >>> get_holding_period_threshold(AssetClass.DEBT)
        730
    """
    if asset_class == AssetClass.EQUITY:
        return 365  # >12 months
    elif asset_class == AssetClass.DEBT:
        return 730  # >24 months (old rule)
    elif asset_class == AssetClass.HYBRID:
        # Hybrid treated as debt for CG purposes
        return 730
    else:
        return 365  # Default to equity threshold


def is_elss_scheme(scheme_name: str) -> bool:
    """
    Check if a scheme is ELSS (Equity Linked Savings Scheme).

    ELSS has 3-year lock-in period and Section 80C benefits.

    Args:
        scheme_name: Scheme name

    Returns:
        True if ELSS scheme
    """
    scheme_upper = scheme_name.upper()
    return 'ELSS' in scheme_upper or 'TAX SAVER' in scheme_upper or 'TAX SAVING' in scheme_upper
