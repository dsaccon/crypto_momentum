def book_query(
        symbol,
        target_vol,
        side='BUY',
        asset_type='spot',
        target_pct=1,
        client=None) -> tuple:
    """
    Return:

    (
        vol_found: bool,
        total_vol: float,
        l0_book_price: float,
        avg_price: float,
        slip_pct: float
    )

    """
    _side = 'asks' if side == 'BUY' else 'bids'
    book_levels = (100, 500, 1000, 5000) # Binance-specific
    get_accum_vol = lambda a: sum([_[1] for _ in a])
    for book_level in book_levels:
        #
        book = client.get_book(symbol=symbol, depth=book_level, asset_type=asset_type)
        accum = []
        pre_trim_vol = 0
        for level in book[_side]:
            accum.append((float(level[0]), float(level[1])))
            if get_accum_vol(accum) >= target_vol:
                trim = get_accum_vol(accum) - target_vol
                pre_trim_vol = get_accum_vol(accum)
                accum[-1] = (accum[-1][0], accum[-1][1] - trim)
                break
        if get_accum_vol(accum) >= target_vol:
            # Current book has sufficient vol
            break
        elif book_level == book_levels[-1]:
            # Not possible to get target_vol
            return (False, get_accum_vol(accum), None, None)
        else:
            # Current level not sufficient. Call again the book at a deeper lev
            pass
    avg_price = sum([_[0]*_[1] for _ in accum])/target_vol
    slip = abs(float(book[_side][0][0]) - avg_price)
    slip_pct = (slip/float(book[_side][0][0]))*100
    results = (get_accum_vol(accum), book[_side][0][0], avg_price, slip_pct)
    if slip_pct < target_pct:
        return (True,) + results
    else:
        return (False,) + results

if __name__ == '__main__':
    from exchanges.binance import BinanceAPI
    client = BinanceAPI()
    result = book_query('LTCUSDT', 10000, side='BUY', asset_type='futures', client=client)
    print(result)
