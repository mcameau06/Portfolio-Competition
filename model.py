import pandas as pd
import numpy as np


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    mean = df.mean()
    std = df.std()
    return (df - mean) / std


if __name__ == "__main__":
    df = pd.read_csv("metrics.csv")
    # drop rows with missing values for now
    df = df.dropna()
    # Momentum, Volatility, Quality, Value, Market Cap
    weights = np.array([0.2, 0.2, 0.2, 0.2, 0.2])

    metrics_columns = ['Price to Earnings', 'Price to Book', 'Earnings Yield', 'Return on Equity', 'Debt to Equity', 'Market Cap','Momentum 3m', 
    'Momentum 6m', 'Volatility Daily', 'Volatility Annual']


    # normalize all metrics
    for column in metrics_columns:df[column] = normalize(df[column])


    # calculate overall factor metrics, will come up with better ones later
    df["Overall Momentum"] = normalize((df["Momentum 3m"] + df["Momentum 6m"])/2)
    df["Overall Volatility"] = normalize(-(df["Volatility Daily"] + df["Volatility Annual"])/2)

    # return on equity is positive, debt to equity is negative to avoid companies with high debt
    df["Overall Quality"] = normalize((df["Return on Equity"] - df["Debt to Equity"])/2)
    df["Overall Value"] = normalize((df["Earnings Yield"] + df["Price to Book"])/2)

    values = np.array(df[['Overall Momentum', 'Overall Volatility', 'Overall Quality', 'Overall Value','Market Cap']])

    # take inner product of weights and normalized df to get score
    df['Score'] = values @ weights


    df.sort_values(by=['Sector', 'Score'], ascending=False,inplace=True)
    final_df= df[['Symbol', 'Name', 'Sector', 'Score','Overall Momentum', 'Overall Volatility', 'Overall Quality', 'Overall Value','Market Cap']]
    final_df.to_csv("scores.csv", index=False)


