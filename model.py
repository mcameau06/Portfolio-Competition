import pandas as pd
import numpy as np

# use z-score normalization, setting a cap at 2.5 std 
def normalize(df):
    mean = df.mean()
    std = df.std()

    z_score = (df - mean) / std

    return z_score.clip(-2.5,2.5)

def calculate_factors(df):
# calculate overall factor metrics, will come up with better ones later


    df["Overall_Momentum"] = (df["Momentum_1m"] + df["Momentum_3m"])/2

    df["Overall_Volatility"] = -(df["Volatility_Daily"] + df["Volatility_Annual"])/2

    # return on equity is positive, debt to equity is negative to avoid companies with high debt
    df["Overall_Quality"] = (df["Return_on_Equity"] - df["Debt_to_Equity"])/2

    df["Overall_Value"] = (df["Earnings_Yield"] + df["Price_to_Book"])/2

    return df


def calculate_scores(df,weights:np.array):
    """
    weight must be a 5x1 column vector where:
    [momentum, volatility, quality, value, market cap]
    Multiplies factors by weights to create score for each stock

    For instance if the weights are all .2, for one stock a score 
    would be a linear combination of :
    Score = beta_1 * .2 + beta_2 * .2 + beta_3 * .2 + beta_4 * .2 + beta_5 * .2
    """

    # creates a M X 5 matrix where M is the number of stocks and 5 is the number of factors
    values = np.array(df[['Overall_Momentum', 'Overall_Volatility', 'Overall_Quality', 'Overall_Value','Market_Cap']])

    df["Score"] = values @ weights

    return df

def build_scores(metrics_file_path,weights:np.array):

    df =  pd.read_csv(metrics_file_path)

    df = df.dropna()

    metrics_columns = ['Price_to_Earnings', 'Price_to_Book', 'Earnings_Yield', 'Return_on_Equity', 'Debt_to_Equity', 'Market_Cap','Momentum_1m', 
    'Momentum_3m', 'Volatility_Daily', 'Volatility_Annual']

    for column in metrics_columns:
        # only normalizing by sector
        df[column] = df.groupby('Sector')[column].transform(normalize)

    df = calculate_factors(df)

    df = calculate_scores(df,weights)

    df = df.sort_values(by=['Sector', 'Score'], ascending=False)
    
    final_df= df[['Symbol', 'Name', 'Sector', 'Score','Overall_Momentum', 'Overall_Volatility', 'Overall_Quality', 'Overall_Value','Market_Cap']]
    
    return final_df




