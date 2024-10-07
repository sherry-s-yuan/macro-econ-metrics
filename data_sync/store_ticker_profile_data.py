# Store ticker basic information into postres DB
import yfinance as yf

# Info to store:
# 1. sector weighting (realestate, consumer_cyclical, basic_materials, consumer_defensive, technology, communication_services, financial_services, utilities, industrials, energy, healthcare)
# 2. investment type weighting: (cash, stock, bond)
 
def get_ticker_data(tickerName: str):
    ticker = yf.Ticker(tickerName)
    # hist = ticker.history(period="1d")

    print("asset_classes")
    print(ticker.funds_data.asset_classes)
    print("top_holdings")
    print(ticker.funds_data.top_holdings)
    print("equity_holdings")
    print(ticker.funds_data.equity_holdings)
    print("bond_holdings")
    print(ticker.funds_data.bond_holdings)
    print("sector_weightings")
    print(ticker.funds_data.sector_weightings)
    
get_ticker_data("SPY")
# get_ticker_data("QQQ")
