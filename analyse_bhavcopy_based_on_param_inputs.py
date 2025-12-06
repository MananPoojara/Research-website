import os
import glob
import datetime
import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Creates LogFile - to be called on each symbol within any function
logFile = []
def createLogFile(symbol, reason, prev_expiry=None, curr_expiry=None, _from=None, _to=None):
    global logFile
    logFile.append({
        'Symbol' : symbol,
        "PrevExpiry" : prev_expiry,
        "CurrExpiry" : curr_expiry,
        "Reason" : reason,
        "From" : _from,
        "To" : _to
    })

# Creates Expiry File for each Ticker
def create_expiry_file():
    files_to_analyze = glob.glob(os.path.join("./cleaned_csvs", "*.csv"))  
    expiry_df = []

    if(len(files_to_analyze)==0):
        print("No csv files found in cleaned_files")
    
    counter = 0
    for file in files_to_analyze:
        print(f"{counter+1} out of {len(files_to_analyze)}")
        counter += 1
        df = pd.read_csv(file)
        df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'], format='%Y-%m-%d')
    
        for symbol in df['Symbol'].unique():
        # for symbol in ['MIDCPNIFTY']:
            filtered_df = df[df['Symbol']==symbol].sort_values(by='ExpiryDate')
            if filtered_df.empty:
                continue

            for expiry in filtered_df['ExpiryDate'].unique():
                expiry_df.append({
                    'Symbol' : symbol,
                    'Expiry' : expiry
                })

    if expiry_df:
        expiry_df = pd.DataFrame(expiry_df)
        expiry_df = expiry_df.drop_duplicates(subset=['Symbol', 'Expiry']).reset_index(drop=True)

        for symbol in expiry_df['Symbol'].unique():
            filtered_df = expiry_df[expiry_df['Symbol']==symbol].sort_values(by='Expiry')
            filtered_df['Current Expiry'] = filtered_df["Expiry"]
            filtered_df['Previous Expiry'] = filtered_df["Expiry"].shift(1)
            filtered_df['Next Expiry'] = filtered_df["Expiry"].shift(-1)
            filtered_df = filtered_df[['Symbol', 'Previous Expiry', 'Current Expiry', 'Next Expiry']].dropna(subset=['Current Expiry'])
            filtered_df = filtered_df.sort_values(by='Current Expiry').reset_index(drop=True)
            path = "./expiryData"
            if not os.path.exists(path):
                os.mkdir(path)
        
            filtered_df.to_csv(path +"/" + symbol + ".csv", index=False)


   
# Selects Strike Data file based on symbol name and returns the data (empty if no data found for symbol)
def getStrikeData(symbol):
    if symbol in ['NIFTY']:
        fileName = "Nifty_strike_data.csv"
    elif symbol in ['BANKNIFTY', 'MIDCPNIFTY']:
        fileName = "Index_strike_data.csv"
    else:
        fileName = "Nifty 50_strike_data.csv"
        
    try:
        df = pd.read_csv(f"./strikeData/{fileName}") 
    except:
        print(f"{fileName} not found in strikeData folder")
        return pd.DataFrame()
    
    for col in ['Ticker', 'Date', 'Close']:
        if col not in df.columns:
            print(f"Column:{col} missing in {fileName}")
            return pd.DataFrame()


    format_list = ["%Y-%m-%d", "%d-%m-%Y", "%y-%m-%d", "%d-%m-%y", "%d-%b-%Y", "%d-%b-%y"]
    for format_type in format_list:
        try:
            df['Date'] = pd.to_datetime(df['Date'], format=format_type, errors="raise")
            break
        except:
            continue

    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        print("Could not convert Date column into Datetime Format.")
        return pd.DataFrame()
    
    df  =   df[(df['Ticker']==symbol)]\
            .drop_duplicates(subset=['Date', 'Ticker'], keep='last')\
            .sort_values(by='Date')\
            .reset_index(drop=True)
         
    return df


# Encapsulated function for processing Params File
def process_params():
    try:
        params_df = pd.read_csv("./params/params.csv")
    except:
        print("params.csv not found in params folder")
        time.sleep(2)
        sys.exit()
    
    
    for col in ['FromDate', 'ToDate']:
        if col not in params_df.columns:
            print(f"{col} not present in params.csv")
            time.sleep(2)
            sys.exit()


    format_list = ["%Y-%m-%d", "%d-%m-%Y", "%y-%m-%d", "%d-%m-%y", "%d-%b-%Y", "%d-%b-%y"]
    for format_type in format_list:
        try:
            params_df['FromDate'] = pd.to_datetime(params_df['FromDate'], format=format_type, errors="raise")
            break
        except:
            continue
    
    for format_type in format_list:
        try:
            params_df['ToDate'] = pd.to_datetime(params_df['ToDate'], format=format_type, errors="raise")
            break
        except:
            continue

    for col in ['FromDate', 'ToDate']:
        if not pd.api.types.is_datetime64_any_dtype(params_df[col]):
            print(f"Not able to convert Param.csv {col} into datetime")
            time.sleep(2)
            sys.exit()
    
    return params_df


# Tilted with RollOver
def analyse_data_with_rollover():  
    params_df = process_params()
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "STK"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        expiryBasis = row['ExpiryBasis']
        weeklyCond = not pd.isna(row['Weekly'])
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']
        
        liquidCond = True
        if symbol in ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "NIFTYNXT50"]:
            suffix = "IDX"
        else:
            weeklyCond = False


        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        fut_expiry_df = pd.DataFrame()

        if not weeklyCond:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        else:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}.csv")
            fut_expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
    
        expiry_df = expiry_df[
                                (expiry_df['Previous Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        if(len(fut_expiry_df)>0):
            fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
            fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)    
       
        
        # Iterate through expiry file (Monthly/Weekly)
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            
            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue

            # Filter strike Data for Spot value and Percentage Change Condition
            filtered_data = df[
                                (df['Date']>=prev_expiry)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None
                
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                time.sleep(2)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if prev_expiry < d < curr_expiry
                    ]

                    start = prev_expiry
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((prev_expiry, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((prev_expiry, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          

            print(f"(Tilted SF) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
            
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")

                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
                
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()    
                call_turnover_val, put_turnover_val = None, None
                call_strike, put_strike = None, None
                call_net, put_net, fut_net = None, None, None
                total_net = None

                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                                
                if weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    fut_expiry = fut_expiry_df[
                                    (fut_expiry_df['Current Expiry']>=curr_expiry)
                                ].sort_values(by='Current Expiry').reset_index(drop=True)
                    
                    if fut_expiry.empty:
                        reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue

                    fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                    bhav_df1_Fut = bhav_df1_Fut[
                                        (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df1_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)

                
                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    bhav_df1_Fut = bhav_df1_Fut[bhav_df1_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                
                if bhav_df1.empty or bhav_df1_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue


                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                if weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    fut_expiry = fut_expiry_df[
                                    (fut_expiry_df['Current Expiry']>=curr_expiry)
                                ].sort_values(by='Current Expiry').reset_index(drop=True)
                    
                    if fut_expiry.empty:
                        reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    
                    fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                    bhav_df2_Fut = bhav_df2_Fut[
                                        (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df2_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)
                    
        
                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    bhav_df2_Fut = bhav_df2_Fut[bhav_df2_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                
                if bhav_df2.empty or bhav_df2_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                # Now Filter the file using from and to date from intervals_df
                furtherFilter = filtered_data[
                                        (filtered_data['Date']>=fromDate)
                                        & (filtered_data['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                          
                
                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
              
                # Get Put Data First for entry Date
                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                                & (bhav_df1['StrikePrice']>=entrySpot)
                                & (bhav_df1['TurnOver']>0)
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                if put_data.empty:
                    reason = f"No put data found above {entrySpot}. Skipping the Trade."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                if (not put_data.empty) and (not pd.isna(put_data.iloc[0]['Close'])):
                    put_strike = put_data.iloc[0]['StrikePrice']
                elif (not put_data.empty) and pd.isna(put_data.iloc[0]['Close']):
                    strike_with_null = put_data.iloc[0]['StrikePrice']
                    found = False
                    same_day_df = bhav_df1[
                        (bhav_df1['Instrument']=="OPT"+suffix) 
                        & (bhav_df1['OptionType']=="PE")
                        & (bhav_df1['StrikePrice']>=strike_with_null)
                        & (bhav_df1['TurnOver']>0)
                    ].sort_values(by='StrikePrice').dropna(subset=['Close'])
                    
                    unique_strikes = sorted(same_day_df['StrikePrice'].unique())
                    strikeFound = None
                    
                    for strike in unique_strikes:
                        temp_df = same_day_df[same_day_df['StrikePrice']==strike]
                        if (not temp_df.empty) and (not pd.isna(temp_df.iloc[0]['Close'])):
                            found = True
                            strikeFound = strike
                            break

                    if found:
                        print(f"Close Null for {put_strike}. Shifting it to", end=" ")
                        put_strike = strikeFound
                        print(put_strike)
                    else:
                        put_strike = None    
                        
                        while fromDate<toDate:
                            print(f"Shifting {fromDate} to ")
                            fromDate = fromDate + timedelta(days=1) 
                            print(fromDate)

                            if fromDate==toDate:
                                print(f"FromDate not found till {toDate}")
                                break

                            fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                            
                            try:
                                temp_df = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                            except:
                                print(f"{fileName1} not found")
                                continue
                        
                            temp_df['Date'] = pd.to_datetime(temp_df['Date'], format='%Y-%m-%d')
                            temp_df['ExpiryDate'] = pd.to_datetime(temp_df['ExpiryDate'], format='%Y-%m-%d')
                            
                            if weeklyCond:
                                bhav_df1_Fut = temp_df.copy(deep=True)
                                fut_expiry = fut_expiry_df[
                                                (fut_expiry_df['Current Expiry']>=curr_expiry)
                                            ].sort_values(by='Current Expiry').reset_index(drop=True)
                                
                                if fut_expiry.empty:
                                    print(f"Fut Expiry not found in {expiryBasis}_Monthly.csv above or on {curr_expiry}")
                                    continue

                                fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                                bhav_df1_Fut = bhav_df1_Fut[
                                                    (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                                    & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                                    & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                                    & (bhav_df1_Fut['Symbol']==symbol)
                                                ].reset_index(drop=True)
        
                            
                            bhav_df1 = temp_df[
                                            (temp_df['Symbol'] == symbol)
                                            (
                                                (temp_df['ExpiryDate']==curr_expiry)
                                                | (temp_df['ExpiryDate']==curr_expiry+timedelta(days=1))
                                                | (temp_df['ExpiryDate']==curr_expiry-timedelta(days=1))
                                            )
                                            & (temp_df['TurnOver']>0)
                                        ].reset_index(drop=True).copy(deep=True)
                            
                            if not weeklyCond:
                                bhav_df1_Fut = bhav_df1.copy(deep=True)
                                bhav_df1_Fut = bhav_df1_Fut[bhav_df1_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)
                            
                            if bhav_df1.empty or bhav_df1_Fut.empty:
                                print(f"Data not found in {fileName1}")
                                print(bhav_df1)
                                print(bhav_df1_Fut)
                                time.sleep(2)
                                continue

                            
                            put_df = bhav_df1[
                                        (bhav_df1['OptionType']=="PE")
                                        & (bhav_df1['Instrument']=="OPT"+suffix)
                                    ].sort_values(by=['Date','StrikePrice']).dropna(subset='Close').reset_index(drop=True).copy(deep=True)
                            
                            if put_df.empty:
                                print(f"Put Data not found in {fileName1}")
                                continue

                            next_day_strike_data = filtered_data[
                                                        (filtered_data['Date']>=fromDate)
                                                    ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                            
                            if next_day_strike_data.empty:
                                print(f"Spot Data not found from {fromDate}")
                                continue
                            
                            if next_day_strike_data.iloc[0]['Date']!=fromDate:
                                print(f"Spot not found for {fromDate}")
                                continue

                            entrySpot = next_day_strike_data.iloc[0]['Close']                    
                            filtered_put_df = put_df[
                                                    (put_df['StrikePrice']>=entrySpot)
                                                    & (put_df['TurnOver']>0)
                                                ].sort_values(by='StrikePrice')
                            
                            if filtered_put_df.empty:
                                print(f"Put data above {entrySpot} not found for {fromDate}")
                                time.sleep(2)
                                continue
                            
                            put_strike = filtered_put_df.iloc[0]['StrikePrice']
            

                if put_strike is None:
                    reason = "Issue encountered when shifting to next date or shifting strike"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                put_entry_data = bhav_df1[
                                    (bhav_df1['StrikePrice']==put_strike)
                                    & (bhav_df1['Instrument']=="OPT"+suffix)
                                    & (bhav_df1['OptionType']=="PE")
                                ]
                put_exit_data = bhav_df2[
                                    (bhav_df2['StrikePrice']==put_strike)
                                    & (bhav_df2['Instrument']=="OPT"+suffix)
                                    & (bhav_df2['OptionType']=="PE")
                                ]
                
                fut_entry_data = bhav_df1_Fut.copy(deep=True)
                fut_exit_data = bhav_df2_Fut.copy(deep=True)
            
                if put_entry_data.empty or put_exit_data.empty:
                    reason =f"Put entry Data not found " if put_entry_data.empty else f"Put exit Data not found for strike {put_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     

                elif fut_entry_data.empty or fut_exit_data.empty:
                    reason =f"Fut entry Data not found " if fut_entry_data.empty else f"Fut exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                put_intrinsic_val = round(put_strike - entrySpot, 2)
                put_intrinsic_val = 0 if put_intrinsic_val<0 else put_intrinsic_val
                put_time_val = round(put_entry_data.iloc[0]['Close'] - put_intrinsic_val, 2)
                put_turnover_val = put_entry_data.iloc[0]['TurnOver']
               
                call_data = bhav_df1[
                                    (bhav_df1['Instrument']=="OPT"+suffix)
                                    & (bhav_df1['OptionType']=="CE")
                                ].dropna(subset='Close').sort_values(by='StrikePrice').copy(deep=True)
                
                if call_data.empty:
                    reason = "Call Entry Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                call_data['diff'] = (call_data['Close'] - put_time_val).abs()
                
                if liquidCond:
                    call_data = call_data[
                                    (call_data['TurnOver']>0)
                                    & (call_data['StrikePrice']>=entrySpot*(1-0.03))
                                    & (call_data['StrikePrice']<=entrySpot*(1+0.03))
                                ]
                
                if call_data.empty:
                    reason = "Data not found for StrikePrice with 3Pct Adjustment"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                call_entry_data = call_data[
                                        (call_data['diff']==call_data['diff'].min())       
                                    ].reset_index(drop=True)    
                call_strike = call_entry_data.iloc[0]['StrikePrice']
                call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                
                if pd.isna(call_strike):
                    reason = f"Call Strike found is Null {call_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                call_exit_data = bhav_df2[
                                    (bhav_df2['StrikePrice']==call_strike)
                                    & (bhav_df2['Instrument']=="OPT"+suffix)
                                    & (bhav_df2['OptionType']=="CE")
                                ]
                
                if call_exit_data.empty:
                    reason = "Call Exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                spot_net = round(exitSpot - entrySpot, 2)        
                fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                total_net = fut_net + put_net + call_net
                total_net_with_spot = spot_net + put_net + call_net
                
                analysis_data.append({
                    "Expiry" : curr_expiry,
                    "Entry Date" : fromDate,
                    "Exit Date" : toDate,
                    
                    "Entry Spot" : entrySpot,
                    "Exit Spot" : exitSpot,
                    "Spot P&L" : spot_net,
                    
                    "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                    "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                    "Future P&L": fut_net,

                    "Put Strike" : put_strike,
                    "Put Turnover" : put_turnover_val,
                    "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                    "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                    'Put P&L' : put_net,
                    
                    "Call Strike" : call_strike,
                    "Call Turnover" : call_turnover_val,
                    "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                    "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                    "Call P&L" : call_net,

                    "Total P&L (With Future)" : total_net,
                    "Total P&L(With Spot)" : total_net_with_spot
                })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', "NIFTYNXT50"]:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Tilted", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Weekly_Tilted", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Tilted", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Monthly_Tilted", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Tilted", symbol)
                else:
                    path = os.path.join("Output", "STK_Monthly_Tilted", symbol)


            os.makedirs(path, exist_ok=True)    
            fileName =  f"{symbol}_summary"
            
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            
            fileName = fileName +"_Tilted"
            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")


        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Tilted", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Weekly_Tilted", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Tilted", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Monthly_Tilted", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Tilted", symbol)
                else:
                    path = os.path.join("Output", "STK_Monthly_Tilted", symbol)

            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary"
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            
            fileName = fileName +"_Tilted_Log"

            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()


# ATM Call and ATM Put - Normal with Rollover
def analyse_data():  
    params_df = process_params()
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "STK"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        expiryBasis = row['ExpiryBasis']
        weeklyCond = not pd.isna(row['Weekly'])
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']
        
        liquidCond = True
        if symbol in ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "NIFTYNXT50"]:
            suffix = "IDX"
        else:
            weeklyCond = False


        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        fut_expiry_df = pd.DataFrame()

        if not weeklyCond:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        else:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}.csv")
            fut_expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
    
        expiry_df = expiry_df[
                                (expiry_df['Previous Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        if(len(fut_expiry_df)>0):
            fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
            fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)
       
        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            

            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue


            # Filter strike Data for Spot value and Percentage Change Condition
            filtered_data = df[
                                (df['Date']>=prev_expiry)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None
                
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if prev_expiry < d < curr_expiry
                    ]

                    start = prev_expiry
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((prev_expiry, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((prev_expiry, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          
            
            
            print(f"(Normal SF) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
            
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:    
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
                
                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
                
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()
                call_turnover_val, put_turnover_val = None, None
                call_strike, put_strike = None, None
                call_net, put_net, fut_net = None, None, None
                total_net = None

                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                                
                if weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    fut_expiry = fut_expiry_df[
                                    (fut_expiry_df['Current Expiry']>=curr_expiry)
                                ].sort_values(by='Current Expiry').reset_index(drop=True)
                    
                    if fut_expiry.empty:
                        reason = f"Fut Expiry not found in {expiryBasis}_Monthly.csv above or on {curr_expiry}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue

                    fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                    bhav_df1_Fut = bhav_df1_Fut[
                                        (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df1_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)

                
                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    bhav_df1_Fut = bhav_df1_Fut[bhav_df1_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                
                if bhav_df1.empty or bhav_df1_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue


                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                if weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    fut_expiry = fut_expiry_df[
                                    (fut_expiry_df['Current Expiry']>=curr_expiry)
                                ].sort_values(by='Current Expiry').reset_index(drop=True)
                    
                    if fut_expiry.empty:
                        reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    
                    fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                    bhav_df2_Fut = bhav_df2_Fut[
                                        (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df2_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)
                    
        
                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    bhav_df2_Fut = bhav_df2_Fut[bhav_df2_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                if bhav_df2.empty or bhav_df2_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                # Now Filter the file using from and to date from intervals_df
                furtherFilter = filtered_data[
                                        (filtered_data['Date']>=fromDate)
                                        & (filtered_data['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                          
                
                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
                

                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                call_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="CE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                if put_data.empty or call_data.empty:
                    reason = f"No put data found." if put_data.empty else f"No Call data found."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue                    

                put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
                call_data['diff'] = abs(call_data['StrikePrice'] - entrySpot)
                put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
                call_target_strike = call_data[call_data['diff']==call_data['diff'].min()].iloc[0]['StrikePrice']
                

                if call_target_strike!=put_target_strike:
                    reason = f"ATM different for Call/Put Call:{call_target_strike} Put:{put_target_strike} Spot:{entrySpot}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                if(call_target_strike==put_target_strike):
                    put_entry_data = bhav_df1[
                        (bhav_df1['StrikePrice']==put_target_strike)
                        & (bhav_df1['Instrument']=="OPT"+suffix)
                        & (bhav_df1['OptionType']=="PE")
                    ]
                    put_exit_data = bhav_df2[
                        (bhav_df2['StrikePrice']==put_target_strike)
                        & (bhav_df2['Instrument']=="OPT"+suffix)
                        & (bhav_df2['OptionType']=="PE")
                    ]

                    call_entry_data = bhav_df1[
                        (bhav_df1['StrikePrice']==call_target_strike)
                        & (bhav_df1['Instrument']=="OPT"+suffix)
                        & (bhav_df1['OptionType']=="CE")
                    ]
                    call_exit_data = bhav_df2[
                        (bhav_df2['StrikePrice']==call_target_strike)
                        & (bhav_df2['Instrument']=="OPT"+suffix)
                        & (bhav_df2['OptionType']=="CE")
                    ]
                
                    fut_entry_data = bhav_df1_Fut.copy(deep=True)
                    fut_exit_data = bhav_df2_Fut.copy(deep=True)
            
                    if put_entry_data.empty or put_exit_data.empty:
                        reason = "Put Entry Data not found" if put_entry_data.empty else f"Put Exit Data not found for strike:{put_target_strike}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue     
                    elif fut_entry_data.empty or fut_exit_data.empty:
                        reason = "FUT Entry Data not found" if fut_entry_data.empty else f"FUT Exit Data not found"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    elif call_entry_data.empty or call_exit_data.empty:
                        reason = "Call Entry Data not found" if call_entry_data.empty else f"Call Exit Data not found for strike:{call_target_strike}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    
                    call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                    put_turnover_val = put_entry_data.iloc[0]['TurnOver']
                    spot_net = round(exitSpot - entrySpot, 2)        
                    fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                    put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                    call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                    total_net = fut_net + put_net + call_net
                    total_net_with_spot = spot_net + put_net + call_net
                
                    analysis_data.append({
                        "Expiry" : curr_expiry,
                        "Entry Date" : fromDate,
                        "Exit Date" : toDate,
                        
                        "Entry Spot" : entrySpot,
                        "Exit Spot" : exitSpot,
                        "Spot P&L" : spot_net,
                        
                        "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                        "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                        "Future P&L": fut_net,

                        "Put Strike" : put_target_strike,
                        "Put Turnover" : put_turnover_val,
                        "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                        "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                        'Put P&L' : put_net,
                        
                        "Call Strike" : call_target_strike,
                        "Call Turnover" : call_turnover_val,
                        "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                        "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                        "Call P&L" : call_net,

                        "Total P&L (With Future)" : total_net,
                        "Total P&L(With Spot)" : total_net_with_spot
                    })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Normal", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Weekly_Normal", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Normal", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Monthly_Normal", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Normal", symbol)
                else:
                    path = os.path.join("Output", "STK_Monthly_Normal", symbol)

            os.makedirs(path, exist_ok=True)    
            fileName =  f"{symbol}_summary"
            
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal"

            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")

        
        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Normal", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Weekly_Normal", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Normal", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Monthly_Normal", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Normal", symbol)
                else:
                    path = os.path.join("Output", "STK_Monthly_Normal", symbol)

            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary"
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal_Log"

            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()


# Put ATM Buy; ATM Put Buy Next Month Expiry
# Sell Call and Put at half of intrinsic value of Put Current Expiry 
# Trade from 5th calendar day till expiry day
# Monthly Expiries Only
# Square off all on expiry Day

def analyse_data_V3(divideBy=2):  
    params_df = process_params()
    
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "STK"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        expiryBasis = row['ExpiryBasis']
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']

        if symbol in ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "NIFTYNXT50"]:
            suffix = "IDX"


        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
    
        expiry_df = expiry_df[
                                (expiry_df['Current Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        # if symbol=="HEROMOTOCO":
        #     print(df)
        #     print(expiry_df)
        # else:
        #     continue

        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            

            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue


            # Filter strike Data for Spot value and Percentage Change Condition
            
            temp_date = pd.Timestamp(curr_expiry.year, curr_expiry.month, 5)
            if temp_date not in df['Date'].values:
                temp_date = df[
                                (df['Date']<=temp_date)
                                & (df['Date'].dt.month==curr_expiry.month)
                                & (df['Date'].dt.year==curr_expiry.year)
                               ].sort_values(by='Date')
                if temp_date.empty:
                    continue

                temp_date = temp_date.iloc[-1]['Date']
            
            filtered_data = df[
                                (df['Date']>=temp_date)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None
                
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if temp_date < d < curr_expiry
                    ]

                    start = temp_date
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((temp_date, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((temp_date, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          
            
            
            print(f"(GammaHunting SF 4 Leg - 3 Put and 1 Call) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
            
 
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:    
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
                       
                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
                
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()
                put_data = pd.DataFrame()
                put_atm_data_entry = pd.DataFrame()
                put_atm_data_exit = pd.DataFrame()
                put_next_expiry_atm_data_entry = pd.DataFrame()
                put_next_expiry_atm_data_exit = pd.DataFrame()
                
                sell_call_data_entry = pd.DataFrame()
                sell_call_data_exit = pd.DataFrame()
                sell_put_data_entry = pd.DataFrame()
                sell_put_data_exit = pd.DataFrame()
                
                put_turnover_val, put_turnover_val_next = None, None
                call_sell_turnover_val, put_sell_turnover_val = None, None
                put_net, put_net_next = None, None
                call_sell_net, put_sell_net = None, None
                total_net, spot_net = None, None

                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                put_next_expiry_atm_data_entry = bhav_df1.copy(deep=True)          

                
                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                put_next_expiry_atm_data_entry = put_next_expiry_atm_data_entry[
                                                    (
                                                        (put_next_expiry_atm_data_entry['ExpiryDate']==next_expiry)
                                                        | (put_next_expiry_atm_data_entry['ExpiryDate']==next_expiry + timedelta(days=1))
                                                        | (put_next_expiry_atm_data_entry['ExpiryDate']==next_expiry - timedelta(days=1))
                                                    )
                                                    & (put_next_expiry_atm_data_entry['Symbol']==symbol)
                            ].reset_index(drop=True)

                
                if bhav_df1.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                elif put_next_expiry_atm_data_entry.empty:
                    reason = f"Data for Next Expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                
                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                put_next_expiry_atm_data_exit = bhav_df2.copy(deep=True)          

                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                put_next_expiry_atm_data_exit = put_next_expiry_atm_data_exit[
                                (
                                    (put_next_expiry_atm_data_exit['ExpiryDate']==next_expiry)
                                    | (put_next_expiry_atm_data_exit['ExpiryDate']==next_expiry-timedelta(days=1))
                                    | (put_next_expiry_atm_data_exit['ExpiryDate']==next_expiry + timedelta(days=1))
                                )
                                & (put_next_expiry_atm_data_exit['Symbol']==symbol)
                            ].reset_index(drop=True)
                
               

                if bhav_df2.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                elif put_next_expiry_atm_data_exit.empty:
                    reason = f"Data for Next Expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                
                # Now Filter the file using from and to date from intervals_df
                furtherFilter = filtered_data[
                                        (filtered_data['Date']>=fromDate)
                                        & (filtered_data['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                          
                            
               
                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
                
                # Get Current Expiry Put Data (For Buying)
                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                # Get Current Expiry Put Data for Selling
                sell_put_data_entry = put_data.copy(deep=True)
                sell_put_data_entry = sell_put_data_entry[sell_put_data_entry['TurnOver']>0].sort_values(by='StrikePrice').reset_index(drop=True)

                # Get Current Expiry Call Data  
                call_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="CE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                # Get Current Expiry Call Data for Selling
                sell_call_data_entry = call_data.copy(deep=True)
                sell_call_data_entry = sell_call_data_entry[sell_call_data_entry['TurnOver']>0].sort_values(by='StrikePrice').reset_index(drop=True)

                
                if put_data.empty or call_data.empty:
                    reason = f"No put data found." if put_data.empty else f"No Call data found."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue                    
                elif sell_put_data_entry.empty or sell_call_data_entry.empty:
                    reason = f"No put data for sell found with TurnOver>0." if sell_put_data_entry.empty else f"No Call data for sell found  with TurnOver>0."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue                    
                
                
                # Get Current Expiry Put ATM entry and exit data 
                put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
                put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
                
                if put_target_strike is None:
                    reason = f"Issue in bhavcopy. Put Strike Null for {put_data['diff'].min()}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue                    

                put_atm_data_entry = put_data[
                                (put_data['StrikePrice']==put_target_strike)
                                ].reset_index(drop=True).copy(deep=True)
                
                put_intrinsic_val = put_atm_data_entry.iloc[0]['StrikePrice'] - entrySpot
                put_intrinsic_val = 0 if put_intrinsic_val<0 else put_intrinsic_val
                put_time_val = round(put_atm_data_entry.iloc[0]['Close'] - put_intrinsic_val, 2)
                price_for_short = put_time_val/divideBy
                
                put_atm_data_exit = bhav_df2[
                                        (bhav_df2['OptionType']=="PE")
                                        & (bhav_df2['StrikePrice']==put_target_strike)
                                    ].reset_index(drop=True).copy(deep=True)
                
                
                sell_call_data_entry['diff'] = abs(sell_call_data_entry['Close'] - price_for_short)
                sell_call_data_entry = sell_call_data_entry[sell_call_data_entry['diff']==sell_call_data_entry['diff'].min()]
            
                sell_call_data_exit = bhav_df2[
                    (bhav_df2['OptionType']=="PE")
                    & (bhav_df2['StrikePrice']==sell_call_data_entry.iloc[0]['StrikePrice'])
                ]
                
                
                sell_put_data_entry['diff'] = abs(sell_put_data_entry['Close'] - price_for_short)
                sell_put_data_entry = sell_put_data_entry[sell_put_data_entry['diff']==sell_put_data_entry['diff'].min()]
                sell_put_data_exit = bhav_df2[
                    (bhav_df2['OptionType']=="PE")
                    & (bhav_df2['StrikePrice']==sell_put_data_entry.iloc[0]['StrikePrice'])
                ]
                

                put_next_expiry_atm_data_entry = put_next_expiry_atm_data_entry[
                    (put_next_expiry_atm_data_entry['StrikePrice']==put_target_strike)
                    & (put_next_expiry_atm_data_entry['Instrument']=="OPT"+suffix)
                    & (put_next_expiry_atm_data_entry['OptionType']=="PE")
                ]
                put_next_expiry_atm_data_exit = put_next_expiry_atm_data_exit[
                    (put_next_expiry_atm_data_exit['StrikePrice']==put_target_strike)
                    & (put_next_expiry_atm_data_exit['Instrument']=="OPT"+suffix)
                    & (put_next_expiry_atm_data_exit['OptionType']=="PE")
                ]

                if put_next_expiry_atm_data_entry.empty or put_next_expiry_atm_data_exit.empty:
                    reason = "No Entry data found for next expiry ATM Put." if put_next_expiry_atm_data_entry.empty else "No Exit data found for next expiry ATM Put."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                
                put_turnover_val = put_atm_data_entry.iloc[0]['TurnOver']
                
                put_turnover_val_next = put_next_expiry_atm_data_entry.iloc[0]['TurnOver']
                call_sell_turnover_val = sell_call_data_entry.iloc[0]['TurnOver']
                put_sell_turnover_val = sell_put_data_entry.iloc[0]['TurnOver']
                
                spot_net = round(exitSpot - entrySpot, 2)        
                put_net = round(put_atm_data_exit.iloc[0]['Close'] - put_atm_data_entry.iloc[0]['Close'], 2)
                put_net_next = round(put_next_expiry_atm_data_exit.iloc[0]['Close'] - put_next_expiry_atm_data_entry.iloc[0]['Close'], 2)
                call_sell_net = round(sell_call_data_entry.iloc[0]['Close'] - sell_call_data_exit.iloc[0]['Close'], 2)
                put_sell_net = round(sell_put_data_entry.iloc[0]['Close'] - sell_put_data_exit.iloc[0]['Close'], 2)
                
                total_net = spot_net + put_net + put_net_next + call_sell_net + put_sell_net 
            
                analysis_data.append({
                    "Expiry" : curr_expiry,
                    "Entry Date" : fromDate,
                    "Exit Date" : toDate,
                    
                    "Entry Spot" : entrySpot,
                    "Exit Spot" : exitSpot,
                    "Spot P&L" : spot_net,
                    
                    "Put Strike(CurrentExpiry)" : put_target_strike,
                    "Put Turnover(CurrentExpiry)" : put_turnover_val,
                    "Put EntryPrice(CurrentExpiry)" : put_atm_data_entry.iloc[0]['Close'],
                    "Put ExitPrice(CurrentExpiry)" : put_atm_data_exit.iloc[0]['Close'],
                    'Put P&L(CurrentExpiry)' : put_net,

                    "Put Strike(NextExpiry)" : put_target_strike,
                    "Put Turnover(NextExpiry)" : put_turnover_val_next,
                    "Put EntryPrice(NextExpiry)" : put_next_expiry_atm_data_entry.iloc[0]['Close'],
                    "Put ExitPrice(NextExpiry)" : put_next_expiry_atm_data_exit.iloc[0]['Close'],
                    'Put P&L(NextExpiry)' : put_net_next,
                    
                    "Call Strike(CurrentExpiry) Short" : sell_call_data_entry.iloc[0]['StrikePrice'],
                    "Call Turnover(CurrentExpiry) Short" : call_sell_turnover_val,
                    "Call EntryPrice(CurrentExpiry) Short" : sell_call_data_entry.iloc[0]['Close'],
                    "Call ExitPrice(CurrentExpiry) Short" : sell_call_data_exit.iloc[0]['Close'],
                    "Call P&L(CurrentExpiry) Short" : call_sell_net,

                    "Put Strike(CurrentExpiry) Short" : sell_put_data_entry.iloc[0]['StrikePrice'],
                    "Put Turnover(CurrentExpiry) Short" : put_sell_turnover_val,
                    "Put EntryPrice(CurrentExpiry) Short" : sell_put_data_entry.iloc[0]['Close'],
                    "Put ExitPrice(CurrentExpiry) Short" : sell_put_data_exit.iloc[0]['Close'],
                    "Put P&L(CurrentExpiry) Short" : put_sell_net,

                    "Total P&L (Gamma Hunting)" : total_net,                    
                })
    
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:     
                if pctChgCond:
                    path = os.path.join("Output", f"IDX_Monthly_GammaHunting_{pctParam}_Pct_Chg", symbol)    
                else:
                    path = os.path.join("Output", "IDX_Monthly_GammaHunting", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_GammaHunting_{pctParam}_Pct_Chg", symbol)    
                else:
                    path = os.path.join("Output", "STK_Monthly_GammaHunting", symbol)
            

            os.makedirs(path, exist_ok=True)    
            fileName =  f"{symbol}_summary_monthly_GammaHunting"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            
            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")

        
        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:     
                if pctChgCond:
                    path = os.path.join("Output", f"IDX_Monthly_GammaHunting_{pctParam}_Pct_Chg", symbol)    
                else:
                    path = os.path.join("Output", "IDX_Monthly_GammaHunting", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_GammaHunting_{pctParam}_Pct_Chg", symbol)    
                else:
                    path = os.path.join("Output", "STK_Monthly_GammaHunting", symbol)

            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary_monthly_GammaHunting"
            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Log"    
            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()


# T-1 to T-1 - Weekly Expiry Nifty only
def analyse_data_Nifty_version3():
    params_df = process_params()
    params_df = params_df.drop_duplicates(subset=['Ticker', 'PctChg']).reset_index(drop=True)
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "IDX"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']
        
        if symbol not in ['NIFTY']:
            reason = f"Unrecognzied {symbol} for Weekly T-1 to T-1"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        liquidCond = True
        
        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').drop_duplicates(subset=['Date']).reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        expiry_df = pd.read_csv(f"./expiryData/NIFTY.csv")
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
        expiry_df = expiry_df[
                                (expiry_df['Previous Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        fut_expiry_df = pd.read_csv(f"./expiryData/NIFTY_Monthly.csv")
        fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
        fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)
        
        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            
            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null" 
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue

            # Filter strike Data for Spot value and Percentage Change Condition
            filtered_data = df[
                                (df['Date']>=prev_expiry)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None
            
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if prev_expiry < d < curr_expiry
                    ]

                    start = prev_expiry
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((prev_expiry, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((prev_expiry, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          
            
            print(f"(T-1 to T-1 Normal SF) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
        
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                fromDate = df[df['Date']<=fromDate].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                toDate = df[df['Date']<=toDate].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                
                if len(fromDate)<2:
                    reason = f"No Data before {fromDate}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                if len(toDate)<2:
                    reason = f"No Data before {toDate}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                fromDate = fromDate.iloc[-2]['Date']
                toDate = toDate.iloc[-2]['Date']
               
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:    
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
                
                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
    
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()
                call_target_strike, put_target_strike = None, None
                call_turnover_val, put_turnover_val = None, None
                call_net, put_net, fut_net = None, None, None
                total_net = None
                
                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                                
                bhav_df1_Fut = bhav_df1.copy(deep=True)
                fut_expiry = fut_expiry_df[
                                (fut_expiry_df['Current Expiry']>=curr_expiry)
                            ].sort_values(by='Current Expiry').reset_index(drop=True)
                
                if fut_expiry.empty:
                    reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                
                bhav_df1_Fut = bhav_df1_Fut[
                                    (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                    & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                    & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                    & (bhav_df1_Fut['Symbol']==symbol)
                                ].reset_index(drop=True)

                
                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if bhav_df1.empty or bhav_df1_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue


                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                bhav_df2_Fut = bhav_df2.copy(deep=True)
                bhav_df2_Fut = bhav_df2_Fut[
                                    (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                    & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                    & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                    & (bhav_df2_Fut['Symbol']==symbol)
                                ].reset_index(drop=True)
                
                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                

                if bhav_df2.empty or bhav_df2_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                # Now Filter the file using from and to date from intervals_df
                furtherFilter = df[
                                    (df['Date']>=fromDate)
                                    & (df['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                          
                if furtherFilter.empty:
                    reason = f"Data not present between From and To Date"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
                
                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                call_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="CE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                

                if put_data.empty or call_data.empty:
                    reason = f"No put data found." if put_data.empty else f"No Call data found."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                
                put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
                call_data['diff'] = abs(call_data['StrikePrice'] - entrySpot)
                put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
                call_target_strike = call_data[call_data['diff']==call_data['diff'].min()].iloc[0]['StrikePrice']
                

                if call_target_strike!=put_target_strike:
                    reason = f"ATM different for Call/Put Call:{call_target_strike} Put:{put_target_strike} Spot:{entrySpot}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                if(call_target_strike==put_target_strike):
                    put_entry_data = bhav_df1[
                        (bhav_df1['StrikePrice']==put_target_strike)
                        & (bhav_df1['Instrument']=="OPT"+suffix)
                        & (bhav_df1['OptionType']=="PE")
                    ]
                    put_exit_data = bhav_df2[
                        (bhav_df2['StrikePrice']==put_target_strike)
                        & (bhav_df2['Instrument']=="OPT"+suffix)
                        & (bhav_df2['OptionType']=="PE")
                    ]

                    call_entry_data = bhav_df1[
                        (bhav_df1['StrikePrice']==call_target_strike)
                        & (bhav_df1['Instrument']=="OPT"+suffix)
                        & (bhav_df1['OptionType']=="CE")
                    ]
                    call_exit_data = bhav_df2[
                        (bhav_df2['StrikePrice']==call_target_strike)
                        & (bhav_df2['Instrument']=="OPT"+suffix)
                        & (bhav_df2['OptionType']=="CE")
                    ]
                
                    fut_entry_data = bhav_df1_Fut.copy(deep=True)
                    fut_exit_data = bhav_df2_Fut.copy(deep=True)
            
                    if put_entry_data.empty or put_exit_data.empty:
                        reason = "Put Entry Data not found" if put_entry_data.empty else f"Put Exit Data not found for strike:{put_target_strike}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue     
                    elif fut_entry_data.empty or fut_exit_data.empty:
                        reason = "FUT Entry Data not found" if fut_entry_data.empty else f"FUT Exit Data not found"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    elif call_entry_data.empty or call_exit_data.empty:
                        reason = "Call Entry Data not found" if call_entry_data.empty else f"Call Exit Data not found for strike:{call_target_strike}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    
                    call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                    put_turnover_val = put_entry_data.iloc[0]['TurnOver']
                    spot_net = round(exitSpot - entrySpot, 2)        
                    fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                    put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                    call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                    total_net = fut_net + put_net + call_net
                    total_net_with_spot = spot_net + put_net + call_net
                
                    analysis_data.append({
                        "Expiry" : curr_expiry,
                        "Entry Date" : fromDate,
                        "Exit Date" : toDate,
                        
                        "Entry Spot" : entrySpot,
                        "Exit Spot" : exitSpot,
                        "Spot P&L" : spot_net,
                        
                        "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                        "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                        "Future P&L": fut_net,

                        "Put Strike" : put_target_strike,
                        "Put Turnover" : put_turnover_val,
                        "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                        "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                        'Put P&L' : put_net,
                        
                        "Call Strike" : call_target_strike,
                        "Call Turnover" : call_turnover_val,
                        "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                        "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                        "Call P&L" : call_net,

                        "Total P&L (With Future)" : total_net,
                        "Total P&L(With Spot)" : total_net_with_spot
                    })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if pctChgCond:
                path = os.path.join("Output", f"IDX_Weekly_T-1_to_T-1_{pctParam}_Pct_Chg_Normal", symbol)
            else:    
                path = os.path.join("Output", "IDX_Weekly_T-1_to_T-1_Normal", symbol)
            

            os.makedirs(path, exist_ok=True)    
            
            fileName =  f"{symbol}_summary"
            fileName = fileName + "_weekly_T-1_to_T-1"
           
            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            
            fileName = fileName +"_Normal"

            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{symbol}_summary.csv saved to {path}")

        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if pctChgCond:
                path = os.path.join("Output", f"IDX_Weekly_T-1_to_T-1_{pctParam}_Pct_Chg_Normal", symbol)
            else:    
                path = os.path.join("Output", "IDX_Weekly_T-1_to_T-1_Normal", symbol)
            
            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary"
            fileName = fileName + "_weekly_T-1_to_T-1"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal_Log"

            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()



# On Last Week of expiry if FUT expiry falls on same day it shifts expiry to next month
def analyse_data_Fut_Next_Expiry_On_Last_Week():
    params_df = process_params()
    params_df = params_df.drop_duplicates(subset=['Ticker', 'PctChg']).reset_index(drop=True)
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "IDX"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']
        
        if symbol not in ['NIFTY']:
            reason = f"Unrecognzied {symbol} for Weekly T-1 to T-1"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        liquidCond = True
        
        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').drop_duplicates(subset=['Date']).reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        expiry_df = pd.read_csv(f"./expiryData/NIFTY.csv")
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
        expiry_df = expiry_df[
                                (expiry_df['Previous Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        fut_expiry_df = pd.read_csv(f"./expiryData/NIFTY_Monthly.csv")
        fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
        fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)
       
        
        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            
            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null" 
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue

            # Filter strike Data for Spot value and Percentage Change Condition
            filtered_data = df[
                                (df['Date']>=prev_expiry)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None  
            
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if prev_expiry < d < curr_expiry
                    ]

                    start = prev_expiry
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((prev_expiry, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((prev_expiry, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          
            
            print(f"(Fut NextExpiry On LastWeek - Normal SF) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
        
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:    
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
                
                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
                  
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()
                call_target_strike, put_target_strike = None, None
                call_turnover_val, put_turnover_val = None, None
                call_net, put_net, fut_net = None, None, None
                total_net = None
                
                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                                
                bhav_df1_Fut = bhav_df1.copy(deep=True)
                fut_expiry = fut_expiry_df[
                                (fut_expiry_df['Current Expiry']>=curr_expiry)
                            ].sort_values(by='Current Expiry').reset_index(drop=True)
                
                if fut_expiry.empty:
                    reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                fut_expiry_date = fut_expiry.iloc[0]['Current Expiry']

                if fut_expiry_date==curr_expiry:
                    if len(fut_expiry)>1:
                        fut_expiry_date = fut_expiry.iloc[1]['Current Expiry']
                    else:
                        reason = f"Next Future Expiry not found in NIFTY_Monthly.csv"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue

                bhav_df1_Fut = bhav_df1_Fut[
                                    (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry_date.month)
                                    & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry_date.year)
                                    & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                    & (bhav_df1_Fut['Symbol']==symbol)
                                ].reset_index(drop=True)

                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if bhav_df1.empty or bhav_df1_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue


                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                bhav_df2_Fut = bhav_df2.copy(deep=True)                 
                bhav_df2_Fut = bhav_df2_Fut[
                                    (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry_date.month)
                                    & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry_date.year)
                                    & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                    & (bhav_df2_Fut['Symbol']==symbol)
                                ].reset_index(drop=True)
                
                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                

                if bhav_df2.empty or bhav_df2_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                # Now Filter the file using from and to date from intervals_df
                furtherFilter = df[
                                    (df['Date']>=fromDate)
                                    & (df['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                if furtherFilter.empty:
                    reason = f"Data not present between From and To Date"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
                

                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                call_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="CE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                if put_data.empty or call_data.empty:
                    reason = f"No put data found." if put_data.empty else f"No Call data found."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
                call_data['diff'] = abs(call_data['StrikePrice'] - entrySpot)
                put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
                call_target_strike = call_data[call_data['diff']==call_data['diff'].min()].iloc[0]['StrikePrice']
                

                if call_target_strike!=put_target_strike:
                    reason = f"ATM different for Call/Put Call:{call_target_strike} Put:{put_target_strike} Spot:{entrySpot}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                if(call_target_strike==put_target_strike):
                    put_entry_data = bhav_df1[
                        (bhav_df1['StrikePrice']==put_target_strike)
                        & (bhav_df1['Instrument']=="OPT"+suffix)
                        & (bhav_df1['OptionType']=="PE")
                    ]
                    put_exit_data = bhav_df2[
                        (bhav_df2['StrikePrice']==put_target_strike)
                        & (bhav_df2['Instrument']=="OPT"+suffix)
                        & (bhav_df2['OptionType']=="PE")
                    ]

                    call_entry_data = bhav_df1[
                        (bhav_df1['StrikePrice']==call_target_strike)
                        & (bhav_df1['Instrument']=="OPT"+suffix)
                        & (bhav_df1['OptionType']=="CE")
                    ]
                    call_exit_data = bhav_df2[
                        (bhav_df2['StrikePrice']==call_target_strike)
                        & (bhav_df2['Instrument']=="OPT"+suffix)
                        & (bhav_df2['OptionType']=="CE")
                    ]
                
                    fut_entry_data = bhav_df1_Fut.copy(deep=True)
                    fut_exit_data = bhav_df2_Fut.copy(deep=True)
            
                    if put_entry_data.empty or put_exit_data.empty:
                        reason = "Put Entry Data not found" if put_entry_data.empty else f"Put Exit Data not found for strike:{put_target_strike}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue     
                    elif fut_entry_data.empty or fut_exit_data.empty:
                        reason = "FUT Entry Data not found" if fut_entry_data.empty else f"FUT Exit Data not found"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    elif call_entry_data.empty or call_exit_data.empty:
                        reason = "Call Entry Data not found" if call_entry_data.empty else f"Call Exit Data not found for strike:{call_target_strike}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue
                    
                    call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                    put_turnover_val = put_entry_data.iloc[0]['TurnOver']
                    spot_net = round(exitSpot - entrySpot, 2)        
                    fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                    put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                    call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                    total_net = fut_net + put_net + call_net
                    total_net_with_spot = spot_net + put_net + call_net
                
                    analysis_data.append({
                        "OptionsExpiry" : curr_expiry,
                        "FutureExpiry" : fut_expiry_date,
                        
                        "Entry Date" : fromDate,
                        "Exit Date" : toDate,
                        
                        "Entry Spot" : entrySpot,
                        "Exit Spot" : exitSpot,
                        "Spot P&L" : spot_net,
                        
                        "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                        "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                        "Future P&L": fut_net,

                        "Put Strike" : put_target_strike,
                        "Put Turnover" : put_turnover_val,
                        "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                        "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                        'Put P&L' : put_net,
                        
                        "Call Strike" : call_target_strike,
                        "Call Turnover" : call_turnover_val,
                        "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                        "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                        "Call P&L" : call_net,

                        "Total P&L (With Future)" : total_net,
                        "Total P&L(With Spot)" : total_net_with_spot
                    })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if pctChgCond:
                path = os.path.join("Output", f"IDX_Weekly_NextFutureOnLastWeek_{pctParam}_Pct_Chg_Normal", symbol)
            else:    
                path = os.path.join("Output", "IDX_Weekly_NextFutureOnLastWeek_Normal", symbol)
            

            os.makedirs(path, exist_ok=True)    
            fileName =  f"{symbol}_summary"
            fileName = fileName + "_weekly_NextFutureOnLastWeek"
           
            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal"

            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")

        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if pctChgCond:
                path = os.path.join("Output", f"IDX_Weekly_NextFutureOnLastWeek_{pctParam}_Pct_Chg_Normal", symbol)
            else:    
                path = os.path.join("Output", "IDX_Weekly_NextFutureOnLastWeek_Normal", symbol)
            
            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary"
            fileName = fileName + "_weekly_NextFutureOnLastWeek"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal_Log"

            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()

# T/T-1/T-2/T-3/T-4/T-5 to T For Weekly Expiries - Weekly Only - Current Short and Next Long
def niftyVersion4(daysGap=0):
    params_df = process_params()
    params_df = params_df.drop_duplicates(subset=['Ticker', 'PctChg']).reset_index(drop=True)
    
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "IDX"
        row = params_df.iloc[p]
        
        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        
        if symbol not in ['NIFTY']:
            reason = f"Unrecognzied {symbol} for Weekly T-1 to T-1"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        liquidCond = True
        
        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry Weekly File. Filter based on Start and End Date
        expiry_df = pd.read_csv(f"./expiryData/NIFTY.csv")
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
        expiry_df = expiry_df[
                                (expiry_df['Current Expiry']>=startDate)
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        fut_expiry_df = pd.read_csv(f"./expiryData/NIFTY_Monthly.csv")
        fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
        fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)
        
        
        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            
            
            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null" 
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
            elif pd.isna(next_expiry):
                reason = f"Next Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue

            fileName1 = fileName2 = ""
            fromDate = prev_expiry
            toDate = curr_expiry
            
            if daysGap>0:
                temp_df = df[df['Date']<curr_expiry].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                if len(temp_df)<daysGap:
                    reason = f"Date Missing in Data for T-{daysGap}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)

                fromDate = temp_df.iloc[-daysGap]['Date']
           

            print(f"Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')} NextExpiry:{next_expiry.strftime('%d-%m-%Y')}")
            print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
            
            fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
            fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
    
            bhav_df1  = pd.DataFrame()
            bhav_df2 = pd.DataFrame()
            bhav_df1_next = pd.DataFrame()
            bhav_df2_next = pd.DataFrame()
            bhav_df1_Fut = pd.DataFrame()
            bhav_df2_Fut = pd.DataFrame()
            call_target_strike, put_target_strike = None, None
            call_turnover_val, put_turnover_val = None, None
            call_net, put_net, fut_net = None, None, None
            total_net = None
            fut_expiry = None

            fut_expiry = fut_expiry_df[
                        (fut_expiry_df['Current Expiry']>=curr_expiry)
                    ].sort_values(by='Current Expiry').reset_index(drop=True)
            if fut_expiry.empty:
                reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
            fut_expiry = fut_expiry.iloc[0]['Current Expiry']

            # First Check Entry Bhavcopy and if it is, format it 
            try:
                bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
            except:
                reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
            
            bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
            bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
            
            bhav_df1_Fut = bhav_df1.copy(deep=True)
            bhav_df1_next = bhav_df1.copy(deep=True)
            
            
            bhav_df1_Fut = bhav_df1_Fut[
                            (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                            & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                            & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                            & (bhav_df1_Fut['Symbol']==symbol)
                        ].reset_index(drop=True)
            
            bhav_df1 = bhav_df1[
                        (
                            (bhav_df1['ExpiryDate']==curr_expiry)
                            | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                            | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                        )
                        & (bhav_df1['Symbol']==symbol)
                    ].reset_index(drop=True)
            
            bhav_df1_next = bhav_df1_next[
                        (
                            (bhav_df1_next['ExpiryDate']==next_expiry)
                            | (bhav_df1_next['ExpiryDate']==next_expiry + timedelta(days=1))
                            | (bhav_df1_next['ExpiryDate']==next_expiry - timedelta(days=1))
                        )
                        & (bhav_df1_next['Symbol']==symbol)
                    ].reset_index(drop=True)
            
            if bhav_df1.empty or bhav_df1_Fut.empty or bhav_df1_next.empty:
                reason = f"Data for current/next expiry or Fut not found in {fileName1}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

         

            # Check Exit Bhavcopy and if it is, format it 
            try:
                bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
            except:
                reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
                

            bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
            bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
            
            bhav_df2_Fut = bhav_df2.copy(deep=True)
            bhav_df2_next = bhav_df2.copy(deep=True)
            
            bhav_df2_Fut = bhav_df2_Fut[
                                (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                & (bhav_df2_Fut['Symbol']==symbol)
                            ].reset_index(drop=True)
            
            bhav_df2 = bhav_df2[
                            (
                                (bhav_df2['ExpiryDate']==curr_expiry)
                                | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                            )
                            & (bhav_df2['Symbol']==symbol)
                        ].reset_index(drop=True)
            
            bhav_df2_next = bhav_df2_next[
                            (
                                (bhav_df2_next['ExpiryDate']==next_expiry)
                                | (bhav_df2_next['ExpiryDate']==next_expiry-timedelta(days=1))
                                | (bhav_df2_next['ExpiryDate']==next_expiry + timedelta(days=1))
                            )
                            & (bhav_df2_next['Symbol']==symbol)
                        ].reset_index(drop=True)
            
            if bhav_df2.empty or bhav_df2_Fut.empty or bhav_df2_next.empty:
                reason = f"Data for current/next expiry or Fut not found in {fileName2}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue  
            
       

            # Now Filter the file using from and to date from intervals_df
            furtherFilter = df[
                                (df['Date']>=fromDate)
                                & (df['Date']<=toDate)
                            ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                        
            if furtherFilter.empty:
                reason = f"Data not present between From and To Date"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

            # Get Spot for from and to date
            if (daysGap>0) and len(furtherFilter)<(daysGap+1):
                reason = f"Spot Data issue from {fromDate} to {toDate}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

            entrySpot = furtherFilter.iloc[0]['Close']
            exitSpot = furtherFilter.iloc[-1]['Close']
            
            
            put_data = bhav_df1[
                            (bhav_df1['Instrument']=="OPT"+suffix)
                            & (bhav_df1['OptionType']=="PE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
            
            put_data_next = bhav_df1_next[
                            (bhav_df1_next['Instrument']=="OPT"+suffix)
                            & (bhav_df1_next['OptionType']=="PE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
            
            call_data = bhav_df1[
                            (bhav_df1['Instrument']=="OPT"+suffix)
                            & (bhav_df1['OptionType']=="CE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
            
            call_data_next = bhav_df1_next[
                            (bhav_df1_next['Instrument']=="OPT"+suffix)
                            & (bhav_df1_next['OptionType']=="CE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
           

            if put_data.empty or call_data.empty:
                reason = f"No put data found." if put_data.empty else f"No Call data found."
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
            elif put_data_next.empty or call_data_next.empty:
                reason = f"No put data of Next Expiry found." if put_data_next.empty else f"No Call data of Next Expiry found."
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
                
            put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
            call_data['diff'] = abs(call_data['StrikePrice'] - entrySpot)
            put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
            call_target_strike = call_data[call_data['diff']==call_data['diff'].min()].iloc[0]['StrikePrice']


            if call_target_strike!=put_target_strike:
                reason = f"ATM different for Call/Put Call:{call_target_strike} Put:{put_target_strike} Spot:{entrySpot}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

            if(call_target_strike==put_target_strike):
                # Current Expiry
                put_entry_data = bhav_df1[
                    (bhav_df1['StrikePrice']==put_target_strike)
                    & (bhav_df1['Instrument']=="OPT"+suffix)
                    & (bhav_df1['OptionType']=="PE")
                ]
                put_exit_data = bhav_df2[
                    (bhav_df2['StrikePrice']==put_target_strike)
                    & (bhav_df2['Instrument']=="OPT"+suffix)
                    & (bhav_df2['OptionType']=="PE")
                ]

                call_entry_data = bhav_df1[
                    (bhav_df1['StrikePrice']==call_target_strike)
                    & (bhav_df1['Instrument']=="OPT"+suffix)
                    & (bhav_df1['OptionType']=="CE")
                ]
                call_exit_data = bhav_df2[
                    (bhav_df2['StrikePrice']==call_target_strike)
                    & (bhav_df2['Instrument']=="OPT"+suffix)
                    & (bhav_df2['OptionType']=="CE")
                ]
            
                fut_entry_data = bhav_df1_Fut.copy(deep=True)
                fut_exit_data = bhav_df2_Fut.copy(deep=True)
        
                if put_entry_data.empty or put_exit_data.empty:
                    reason = "Put Entry Data not found" if put_entry_data.empty else f"Put Exit Data not found for strike:{put_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     
                elif fut_entry_data.empty or fut_exit_data.empty:
                    reason = "FUT Entry Data not found" if fut_entry_data.empty else f"FUT Exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                elif call_entry_data.empty or call_exit_data.empty:
                    reason = "Call Entry Data not found" if call_entry_data.empty else f"Call Exit Data not found for strike:{call_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                # Next Expiry
                put_entry_data_next = bhav_df1_next[
                    (bhav_df1_next['StrikePrice']==put_target_strike)
                    & (bhav_df1_next['Instrument']=="OPT"+suffix)
                    & (bhav_df1_next['OptionType']=="PE")
                ]
                put_exit_data_next = bhav_df2_next[
                    (bhav_df2_next['StrikePrice']==put_target_strike)
                    & (bhav_df2_next['Instrument']=="OPT"+suffix)
                    & (bhav_df2_next['OptionType']=="PE")
                ]

                call_entry_data_next = bhav_df1_next[
                    (bhav_df1_next['StrikePrice']==call_target_strike)
                    & (bhav_df1_next['Instrument']=="OPT"+suffix)
                    & (bhav_df1_next['OptionType']=="CE")
                ]
                call_exit_data_next = bhav_df2_next[
                    (bhav_df2_next['StrikePrice']==call_target_strike)
                    & (bhav_df2_next['Instrument']=="OPT"+suffix)
                    & (bhav_df2_next['OptionType']=="CE")
                ]
        
                if put_entry_data_next.empty or put_exit_data_next.empty:
                    reason = "Put Entry Data (Next Expiry) not found" if put_entry_data.empty else f"Put Exit Data (Next Expiry) not found for strike:{put_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     
                elif call_entry_data_next.empty or call_exit_data_next.empty:
                    reason = "Call Entry Data (Next Expiry) not found" if call_entry_data.empty else f"Call Exit Data (Next Expiry) not found for strike:{call_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

               
                call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                call_turnover_val_next = call_entry_data_next.iloc[0]['TurnOver']
                put_turnover_val = put_entry_data.iloc[0]['TurnOver']
                put_turnover_val_next = put_entry_data_next.iloc[0]['TurnOver']

                spot_net = round(exitSpot - entrySpot, 2)        
                fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                
                put_net_next = round(put_entry_data_next.iloc[0]['Close'] - put_exit_data_next.iloc[0]['Close'], 2)
                call_net_next = round(call_exit_data_next.iloc[0]['Close'] - call_entry_data_next.iloc[0]['Close'], 2)
                total_net = put_net + call_net
                total_net_next = put_net_next + call_net_next
                
                
                analysis_data.append({
                    "Expiry" : curr_expiry,
                    "Next Expiry" : next_expiry,
                    
                    "Entry Date" : fromDate,
                    "Exit Date" : toDate,
                    
                    "Entry Spot" : entrySpot,
                    "Exit Spot" : exitSpot,
                    "Spot P&L" : spot_net,
                    
                    "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                    "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                    "Future P&L": fut_net,

                    "Put Strike" : put_target_strike,
                    
                    "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                    "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                    'Put P&L' : put_net,
                    "Put Turnover" : put_turnover_val,
                    
                    "Put EntryPrice(Next Expiry)" : put_entry_data_next.iloc[0]['Close'],
                    "Put ExitPrice(Next Expiry)" : put_exit_data_next.iloc[0]['Close'],
                    'Put P&L(Next Expiry)' : put_net_next,
                    "Put Turnover(Next Expiry)" : put_turnover_val_next,
                    
                    "Call Strike" : call_target_strike,
                    
                    "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                    "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                    "Call P&L" : call_net,
                    "Call Turnover" : call_turnover_val,

                    "Call EntryPrice(Next Expiry)" : call_entry_data_next.iloc[0]['Close'],
                    "Call ExitPrice(Next Expiry)" : call_exit_data_next.iloc[0]['Close'],
                    "Call P&L(Next Expiry)" : call_net_next,
                    "Call Turnover(Next Expiry)" : call_turnover_val_next,

                    "Total P&L(Current Expiry)" : total_net,
                    "Total P&L(Next Expiry)" : total_net_next
                })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = os.path.join("Output", "IDX_T_to_T_Normal_Current&Next", symbol)
            fileName =  f"{symbol}_summary_weekly_T_to_T_Normal_Current&Next"
            if daysGap>0:
                path = os.path.join("Output", f"IDX_T-{daysGap}_to_T_Normal_Current&Next", symbol)
                fileName =  f"{symbol}_summary_weekly_T-{daysGap}_to_T_Normal_Current&Next"
            
            os.makedirs(path, exist_ok=True)    
            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{symbol}_summary.csv saved to {path}")

        if logFile:
            log_df = pd.DataFrame(logFile)
            path = os.path.join("Output", "IDX_T_to_T_Normal_Current&Next", symbol)
            fileName =  f"{symbol}_summary_weekly_T_to_T_Normal_Current&Next_Log"
            if daysGap>0:
                path = os.path.join("Output", f"IDX_T-{daysGap}_to_T_Normal_Current&Next", symbol)
                fileName =  f"{symbol}_summary_weekly_T-{daysGap}_to_T_Normal_Current&Next_Log"
            
            os.makedirs(path, exist_ok=True)     
            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()


# T/T-1/T-2/T-3/T-4/T-5 to T For Weekly Expiries - Weekly Only - Current Short and Next-to-Next Long
def niftyVersion5(daysGap=0):
    params_df = process_params()
    params_df = params_df.drop_duplicates(subset=['Ticker', 'PctChg']).reset_index(drop=True)
    
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "IDX"
        row = params_df.iloc[p]
        
        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        
        if symbol not in ['NIFTY']:
            reason = f"Unrecognzied {symbol} for Weekly T-1 to T-1"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        liquidCond = True
        
        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry Weekly File. Filter based on Start and End Date
        expiry_df = pd.read_csv(f"./expiryData/NIFTY.csv")
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
        expiry_df["Next-To-Next Expiry"] = expiry_df['Next Expiry'].shift(-1)
        
        expiry_df = expiry_df[
                                (expiry_df['Current Expiry']>=startDate)
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        fut_expiry_df = pd.read_csv(f"./expiryData/NIFTY_Monthly.csv")
        fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
        fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
        fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)
        
        
        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next-To-Next Expiry"]
            
            
            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null" 
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
            elif pd.isna(next_expiry):
                reason = f"Next-to-Next Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue

            fileName1 = fileName2 = ""
            fromDate = prev_expiry
            toDate = curr_expiry
            
            if daysGap>0:
                temp_df = df[df['Date']<curr_expiry].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                if len(temp_df)<daysGap:
                    reason = f"Date Missing in Data for T-{daysGap}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)

                fromDate = temp_df.iloc[-daysGap]['Date']
           

            print(f"Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')} NextToNextExpiry:{next_expiry.strftime('%d-%m-%Y')}")
            print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
            
            fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
            fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
    
            bhav_df1  = pd.DataFrame()
            bhav_df2 = pd.DataFrame()
            bhav_df1_next = pd.DataFrame()
            bhav_df2_next = pd.DataFrame()
            bhav_df1_Fut = pd.DataFrame()
            bhav_df2_Fut = pd.DataFrame()
            call_target_strike, put_target_strike = None, None
            call_turnover_val, put_turnover_val = None, None
            call_net, put_net, fut_net = None, None, None
            total_net = None
            fut_expiry = None

            fut_expiry = fut_expiry_df[
                        (fut_expiry_df['Current Expiry']>=curr_expiry)
                    ].sort_values(by='Current Expiry').reset_index(drop=True)
            if fut_expiry.empty:
                reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
            fut_expiry = fut_expiry.iloc[0]['Current Expiry']

            # First Check Entry Bhavcopy and if it is, format it 
            try:
                bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
            except:
                reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
            
            bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
            bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
            
            bhav_df1_Fut = bhav_df1.copy(deep=True)
            bhav_df1_next = bhav_df1.copy(deep=True)
            
            
            bhav_df1_Fut = bhav_df1_Fut[
                            (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                            & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                            & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                            & (bhav_df1_Fut['Symbol']==symbol)
                        ].reset_index(drop=True)
            
            bhav_df1 = bhav_df1[
                        (
                            (bhav_df1['ExpiryDate']==curr_expiry)
                            | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                            | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                        )
                        & (bhav_df1['Symbol']==symbol)
                    ].reset_index(drop=True)
            
            bhav_df1_next = bhav_df1_next[
                        (
                            (bhav_df1_next['ExpiryDate']==next_expiry)
                            | (bhav_df1_next['ExpiryDate']==next_expiry + timedelta(days=1))
                            | (bhav_df1_next['ExpiryDate']==next_expiry - timedelta(days=1))
                        )
                        & (bhav_df1_next['Symbol']==symbol)
                    ].reset_index(drop=True)
            
            if bhav_df1.empty or bhav_df1_Fut.empty or bhav_df1_next.empty:
                reason = f"Data for current/next expiry or Fut not found in {fileName1}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

         

            # Check Exit Bhavcopy and if it is, format it 
            try:
                bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
            except:
                reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
                

            bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
            bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
            
            bhav_df2_Fut = bhav_df2.copy(deep=True)
            bhav_df2_next = bhav_df2.copy(deep=True)
            
            bhav_df2_Fut = bhav_df2_Fut[
                                (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                & (bhav_df2_Fut['Symbol']==symbol)
                            ].reset_index(drop=True)
            
            bhav_df2 = bhav_df2[
                            (
                                (bhav_df2['ExpiryDate']==curr_expiry)
                                | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                            )
                            & (bhav_df2['Symbol']==symbol)
                        ].reset_index(drop=True)
            
            bhav_df2_next = bhav_df2_next[
                            (
                                (bhav_df2_next['ExpiryDate']==next_expiry)
                                | (bhav_df2_next['ExpiryDate']==next_expiry-timedelta(days=1))
                                | (bhav_df2_next['ExpiryDate']==next_expiry + timedelta(days=1))
                            )
                            & (bhav_df2_next['Symbol']==symbol)
                        ].reset_index(drop=True)
            
            if bhav_df2.empty or bhav_df2_Fut.empty or bhav_df2_next.empty:
                reason = f"Data for current/next expiry or Fut not found in {fileName2}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue  
            

            # Now Filter the file using from and to date from intervals_df
            furtherFilter = df[
                                (df['Date']>=fromDate)
                                & (df['Date']<=toDate)
                            ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                        
            if furtherFilter.empty:
                reason = f"Data not present between From and To Date"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

            # Get Spot for from and to date
            if (daysGap>0) and len(furtherFilter)<(daysGap+1):
                reason = f"Spot Data issue from {fromDate} to {toDate}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

            entrySpot = furtherFilter.iloc[0]['Close']
            exitSpot = furtherFilter.iloc[-1]['Close']
                     
            
            put_data = bhav_df1[
                            (bhav_df1['Instrument']=="OPT"+suffix)
                            & (bhav_df1['OptionType']=="PE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
            
            put_data_next = bhav_df1_next[
                            (bhav_df1_next['Instrument']=="OPT"+suffix)
                            & (bhav_df1_next['OptionType']=="PE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
            
            call_data = bhav_df1[
                            (bhav_df1['Instrument']=="OPT"+suffix)
                            & (bhav_df1['OptionType']=="CE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
            
            call_data_next = bhav_df1_next[
                            (bhav_df1_next['Instrument']=="OPT"+suffix)
                            & (bhav_df1_next['OptionType']=="CE")
                        ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
           

            if put_data.empty or call_data.empty:
                reason = f"No put data found." if put_data.empty else f"No Call data found."
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
            elif put_data_next.empty or call_data_next.empty:
                reason = f"No put data of Next Expiry found." if put_data_next.empty else f"No Call data of Next Expiry found."
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue
                
            put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
            call_data['diff'] = abs(call_data['StrikePrice'] - entrySpot)
            put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
            call_target_strike = call_data[call_data['diff']==call_data['diff'].min()].iloc[0]['StrikePrice']


            if call_target_strike!=put_target_strike:
                reason = f"ATM different for Call/Put Call:{call_target_strike} Put:{put_target_strike} Spot:{entrySpot}"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                continue

            if(call_target_strike==put_target_strike):
                # Current Expiry
                put_entry_data = bhav_df1[
                    (bhav_df1['StrikePrice']==put_target_strike)
                    & (bhav_df1['Instrument']=="OPT"+suffix)
                    & (bhav_df1['OptionType']=="PE")
                ]
                put_exit_data = bhav_df2[
                    (bhav_df2['StrikePrice']==put_target_strike)
                    & (bhav_df2['Instrument']=="OPT"+suffix)
                    & (bhav_df2['OptionType']=="PE")
                ]

                call_entry_data = bhav_df1[
                    (bhav_df1['StrikePrice']==call_target_strike)
                    & (bhav_df1['Instrument']=="OPT"+suffix)
                    & (bhav_df1['OptionType']=="CE")
                ]
                call_exit_data = bhav_df2[
                    (bhav_df2['StrikePrice']==call_target_strike)
                    & (bhav_df2['Instrument']=="OPT"+suffix)
                    & (bhav_df2['OptionType']=="CE")
                ]
            
                fut_entry_data = bhav_df1_Fut.copy(deep=True)
                fut_exit_data = bhav_df2_Fut.copy(deep=True)
        
                if put_entry_data.empty or put_exit_data.empty:
                    reason = "Put Entry Data not found" if put_entry_data.empty else f"Put Exit Data not found for strike:{put_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     
                elif fut_entry_data.empty or fut_exit_data.empty:
                    reason = "FUT Entry Data not found" if fut_entry_data.empty else f"FUT Exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                elif call_entry_data.empty or call_exit_data.empty:
                    reason = "Call Entry Data not found" if call_entry_data.empty else f"Call Exit Data not found for strike:{call_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                # Next Expiry
                put_entry_data_next = bhav_df1_next[
                    (bhav_df1_next['StrikePrice']==put_target_strike)
                    & (bhav_df1_next['Instrument']=="OPT"+suffix)
                    & (bhav_df1_next['OptionType']=="PE")
                ]
                put_exit_data_next = bhav_df2_next[
                    (bhav_df2_next['StrikePrice']==put_target_strike)
                    & (bhav_df2_next['Instrument']=="OPT"+suffix)
                    & (bhav_df2_next['OptionType']=="PE")
                ]

                call_entry_data_next = bhav_df1_next[
                    (bhav_df1_next['StrikePrice']==call_target_strike)
                    & (bhav_df1_next['Instrument']=="OPT"+suffix)
                    & (bhav_df1_next['OptionType']=="CE")
                ]
                call_exit_data_next = bhav_df2_next[
                    (bhav_df2_next['StrikePrice']==call_target_strike)
                    & (bhav_df2_next['Instrument']=="OPT"+suffix)
                    & (bhav_df2_next['OptionType']=="CE")
                ]
        
                if put_entry_data_next.empty or put_exit_data_next.empty:
                    reason = "Put Entry Data (Next Expiry) not found" if put_entry_data.empty else f"Put Exit Data (Next Expiry) not found for strike:{put_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     
                elif call_entry_data_next.empty or call_exit_data_next.empty:
                    reason = "Call Entry Data (Next Expiry) not found" if call_entry_data.empty else f"Call Exit Data (Next Expiry) not found for strike:{call_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

               
                call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                call_turnover_val_next = call_entry_data_next.iloc[0]['TurnOver']
                put_turnover_val = put_entry_data.iloc[0]['TurnOver']
                put_turnover_val_next = put_entry_data_next.iloc[0]['TurnOver']

                spot_net = round(exitSpot - entrySpot, 2)        
                fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                
                put_net_next = round(put_entry_data_next.iloc[0]['Close'] - put_exit_data_next.iloc[0]['Close'], 2)
                call_net_next = round(call_exit_data_next.iloc[0]['Close'] - call_entry_data_next.iloc[0]['Close'], 2)
                total_net = put_net + call_net
                total_net_next = put_net_next + call_net_next
                
                
                analysis_data.append({
                    "Expiry" : curr_expiry,
                    "Next-To-Next Expiry" : next_expiry,
                    
                    "Entry Date" : fromDate,
                    "Exit Date" : toDate,
                    
                    "Entry Spot" : entrySpot,
                    "Exit Spot" : exitSpot,
                    "Spot P&L" : spot_net,
                    
                    "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                    "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                    "Future P&L": fut_net,

                    "Put Strike" : put_target_strike,
                    
                    "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                    "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                    'Put P&L' : put_net,
                    "Put Turnover" : put_turnover_val,
                    
                    "Put EntryPrice(Next-To-Next Expiry)" : put_entry_data_next.iloc[0]['Close'],
                    "Put ExitPrice(Next-To-Next Expiry)" : put_exit_data_next.iloc[0]['Close'],
                    'Put P&L(Next-To-Next Expiry)' : put_net_next,
                    "Put Turnover(Next-To-Next Expiry)" : put_turnover_val_next,
                    
                    "Call Strike" : call_target_strike,
                    
                    "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                    "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                    "Call P&L" : call_net,
                    "Call Turnover" : call_turnover_val,

                    "Call EntryPrice(Next-To-Next Expiry)" : call_entry_data_next.iloc[0]['Close'],
                    "Call ExitPrice(Next-To-Next Expiry)" : call_exit_data_next.iloc[0]['Close'],
                    "Call P&L(Next-To-Next Expiry)" : call_net_next,
                    "Call Turnover(Next-To-Next Expiry)" : call_turnover_val_next,

                    "Total P&L(Current Expiry)" : total_net,
                    "Total P&L(Next-To-Next Expiry)" : total_net_next
                })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = os.path.join("Output", "IDX_T_to_T_Normal_Current&Next-To-Next", symbol)
            fileName =  f"{symbol}_summary_weekly_T_to_T_Normal_Current&Next-To-Next"
            if daysGap>0:
                path = os.path.join("Output", f"IDX_T-{daysGap}_to_T_Normal_Current&Next-To-Next", symbol)
                fileName =  f"{symbol}_summary_weekly_T-{daysGap}_to_T_Normal_Current&Next-To-Next"
            
            os.makedirs(path, exist_ok=True)    
            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{symbol}_summary.csv saved to {path}")

        if logFile:
            log_df = pd.DataFrame(logFile)
            path = os.path.join("Output", "IDX_T_to_T_Normal_Current&Next-To-Next", symbol)
            fileName =  f"{symbol}_summary_weekly_T_to_T_Normal_Current&Next-To-Next_Log"
            if daysGap>0:
                path = os.path.join("Output", f"IDX_T-{daysGap}_to_T_Normal_Current&Next-To-Next", symbol)
                fileName =  f"{symbol}_summary_weekly_T-{daysGap}_to_T_Normal_Current&Next-To-Next_Log"
            
            os.makedirs(path, exist_ok=True)     
            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()



# Get all symbol data yearwise
def getallSymbolsData():
    allFiles = glob.glob(os.path.join("./cleaned_csvs", "*.csv"))
    data = []
    count = 0

    for file in allFiles:
        print(f"{count+1} out of {len(allFiles)}")
        count += 1
        df = pd.read_csv(file)
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'], format='%Y-%m-%d')            
        df['Symbol'] = df['Symbol'].str.upper()
        for symbol in df['Symbol'].unique():
            symbol_df = df[df['Symbol'] == symbol].copy()
            symbol_df['Year'] = symbol_df['Date'].dt.year
            out_dir = f"./data/{symbol}"
            os.makedirs(out_dir, exist_ok=True)
            for year in symbol_df['Year'].dropna().unique():
                year = int(year)
                out_file = f"{out_dir}/{year}.csv"
                write_header = not os.path.exists(out_file)
                to_write = symbol_df[symbol_df['Year'] == year].drop(columns=['Year'])
                to_write.to_csv(
                    out_file,
                    mode='a',         
                    header=write_header,
                    index=False
                )



# Put above pct of entrySpot
def analyse_data_with_rollover_V2(pct=0.4):  
    params_df = process_params()
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "STK"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        expiryBasis = row['ExpiryBasis']
        weeklyCond = not pd.isna(row['Weekly'])
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']
        
        liquidCond = True
        if symbol in ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "NIFTYNXT50"]:
            suffix = "IDX"
        else:
            weeklyCond = False


        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        fut_expiry_df = pd.DataFrame()

        if not weeklyCond:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        else:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}.csv")
            fut_expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
    
        expiry_df = expiry_df[
                                (expiry_df['Previous Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        if(len(fut_expiry_df)>0):
            fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
            fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)    
       
        
        # Iterate through expiry file (Monthly/Weekly)
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            
            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue

            # Filter strike Data for Spot value and Percentage Change Condition
            filtered_data = df[
                                (df['Date']>=prev_expiry)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None
                
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                time.sleep(2)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if prev_expiry < d < curr_expiry
                    ]

                    start = prev_expiry
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((prev_expiry, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((prev_expiry, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          

            print(f"(Tilted SF with Put above {pct} Pct  of Spot) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
            
            
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")

                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
                
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()    
                bhav_df1_Fut = pd.DataFrame()
                bhav_df2_Fut = pd.DataFrame()
                call_turnover_val, put_turnover_val = None, None
                call_strike, put_strike = None, None
                call_net, put_net, fut_net = None, None, None
                total_net = None

                
                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                                
                if weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    fut_expiry = fut_expiry_df[
                                    (fut_expiry_df['Current Expiry']>=curr_expiry)
                                ].sort_values(by='Current Expiry').reset_index(drop=True)
                    
                    if fut_expiry.empty:
                        reason = f"Fut Expiry not found in NIFTY_Monthly.csv above or on {curr_expiry}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue

                    fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                    bhav_df1_Fut = bhav_df1_Fut[
                                        (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df1_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)

                
                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    bhav_df1_Fut = bhav_df1_Fut[bhav_df1_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                
                if bhav_df1.empty or bhav_df1_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue


                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                if weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)  
                    bhav_df2_Fut = bhav_df2_Fut[
                                        (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df2_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)
                    
        
                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    bhav_df2_Fut = bhav_df2_Fut[bhav_df2_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                
                if bhav_df2.empty or bhav_df2_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                # Now Filter the file using from and to date from intervals_df
                furtherFilter = filtered_data[
                                        (filtered_data['Date']>=fromDate)
                                        & (filtered_data['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                          
                
                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
                
                # Get Put Data First for entry Date
                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                                & (bhav_df1['StrikePrice']>=(entrySpot*(1-(pct/100))))
                                & (bhav_df1['TurnOver']>0)
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                if put_data.empty:
                    reason = f"No put data found above {entrySpot*((1+(pct/100)))}. Skipping the Trade."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                if (not put_data.empty) and (not pd.isna(put_data.iloc[0]['Close'])):
                    put_strike = put_data.iloc[0]['StrikePrice']
                elif (not put_data.empty) and pd.isna(put_data.iloc[0]['Close']):
                    strike_with_null = put_data.iloc[0]['StrikePrice']
                    found = False
                    
                    same_day_df = bhav_df1[
                        (bhav_df1['Instrument']=="OPT"+suffix) 
                        & (bhav_df1['OptionType']=="PE")
                        & (bhav_df1['StrikePrice']>=strike_with_null)
                        & (bhav_df1['TurnOver']>0)
                    ].sort_values(by='StrikePrice', ascending=False).dropna(subset=['Close'])
                    
                    unique_strikes = sorted(same_day_df['StrikePrice'].unique(), reverse=True)
                    strikeFound = None
                    
                    for strike in unique_strikes:
                        temp_df = same_day_df[same_day_df['StrikePrice']==strike]
                        if (not temp_df.empty) and (not pd.isna(temp_df.iloc[0]['Close'])):
                            found = True
                            strikeFound = strike
                            break

                    if found:
                        print(f"Close Null for {put_strike}. Shifting it to", end=" ")
                        put_strike = strikeFound
                        print(put_strike)
                    else:
                        put_strike = None    
                        
                        while fromDate<toDate:
                            print(f"Shifting {fromDate} to ")
                            fromDate = fromDate + timedelta(days=1) 
                            print(fromDate)

                            if fromDate==toDate:
                                print(f"FromDate not found till {toDate}")
                                break

                            fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                            
                            try:
                                temp_df = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                            except:
                                print(f"{fileName1} not found")
                                continue
                        
                            temp_df['Date'] = pd.to_datetime(temp_df['Date'], format='%Y-%m-%d')
                            temp_df['ExpiryDate'] = pd.to_datetime(temp_df['ExpiryDate'], format='%Y-%m-%d')
                            
                            if weeklyCond:
                                bhav_df1_Fut = temp_df.copy(deep=True)
                                bhav_df1_Fut = bhav_df1_Fut[
                                                    (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                                    & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                                    & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                                    & (bhav_df1_Fut['Symbol']==symbol)
                                                ].reset_index(drop=True)
        
                            
                            bhav_df1 = temp_df[
                                            (temp_df['Symbol'] == symbol)
                                            & (
                                                (temp_df['ExpiryDate']==curr_expiry)
                                                | (temp_df['ExpiryDate']==curr_expiry+timedelta(days=1))
                                                | (temp_df['ExpiryDate']==curr_expiry-timedelta(days=1))
                                            )
                                        ].reset_index(drop=True).copy(deep=True)
                            
                            if not weeklyCond:
                                bhav_df1_Fut = bhav_df1.copy(deep=True)
                                bhav_df1_Fut = bhav_df1_Fut[bhav_df1_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)
                            
                            if bhav_df1.empty or bhav_df1_Fut.empty:
                                reason = f"Data not found in {fileName1}"
                                print(reason)
                                continue

                            
                            put_df = bhav_df1[
                                        (bhav_df1['OptionType']=="PE")
                                        & (bhav_df1['Instrument']=="OPT"+suffix)
                                    ].sort_values(by='StrikePrice', ascending=False).dropna(subset='Close').reset_index(drop=True).copy(deep=True)
                            
                            if put_df.empty:
                                print(f"Put Data not found in {fileName1}")
                                continue

                            next_day_strike_data = filtered_data[
                                                        (filtered_data['Date']>=fromDate)
                                                    ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                            
                            if next_day_strike_data.empty:
                                print(f"Spot Data not found from {fromDate}")
                                continue
                            
                            if next_day_strike_data.iloc[0]['Date']!=fromDate:
                                print(f"Spot not found for {fromDate}")
                                continue

                            entrySpot = next_day_strike_data.iloc[0]['Close']                    
                            filtered_put_df = put_df[
                                                    (put_df['StrikePrice']>=(entrySpot*(1-(pct/100))))
                                                    & (put_df['TurnOver']>0)
                                                ].sort_values(by='StrikePrice', ascending=True)
                            
                            if filtered_put_df.empty:
                                print(f"Put data above {entrySpot*(1+(pct/100))} not found for {fromDate}")
                                time.sleep(2)
                                continue
                            
                            put_strike = filtered_put_df.iloc[0]['StrikePrice']
            

                if put_strike is None:
                    reason = "Issue encountered when shifting to next date or shifting strike"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                put_entry_data = bhav_df1[
                                    (bhav_df1['StrikePrice']==put_strike)
                                    & (bhav_df1['Instrument']=="OPT"+suffix)
                                    & (bhav_df1['OptionType']=="PE")
                                ]
                put_exit_data = bhav_df2[
                                    (bhav_df2['StrikePrice']==put_strike)
                                    & (bhav_df2['Instrument']=="OPT"+suffix)
                                    & (bhav_df2['OptionType']=="PE")
                                ]
                
                fut_entry_data = bhav_df1_Fut.copy(deep=True)
                fut_exit_data = bhav_df2_Fut.copy(deep=True)
            
                if put_entry_data.empty or put_exit_data.empty:
                    reason =f"Put entry Data not found " if put_entry_data.empty else f"Put exit Data not found for strike {put_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     

                elif fut_entry_data.empty or fut_exit_data.empty:
                    reason =f"Fut entry Data not found " if fut_entry_data.empty else f"Fut exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                put_intrinsic_val = round(put_strike - entrySpot, 2)
                put_intrinsic_val = 0 if put_intrinsic_val<0 else put_intrinsic_val
                put_time_val = round(put_entry_data.iloc[0]['Close'] - put_intrinsic_val, 2)
                put_turnover_val = put_entry_data.iloc[0]['TurnOver']
               
                call_data = bhav_df1[
                                    (bhav_df1['Instrument']=="OPT"+suffix)
                                    & (bhav_df1['OptionType']=="CE")
                                ].dropna(subset='Close').sort_values(by='StrikePrice').copy(deep=True)
                
                if call_data.empty:
                    reason = "Call Entry Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                call_data['diff'] = (call_data['Close'] - put_time_val).abs()
                
                if liquidCond:
                    call_data = call_data[
                                    (call_data['TurnOver']>0)
                                    & (call_data['StrikePrice']>=entrySpot*(1-0.03))
                                    & (call_data['StrikePrice']<=entrySpot*(1+0.03))
                                ]
                
                if call_data.empty:
                    reason = "Data not found for StrikePrice with 3Pct Adjustment"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                call_entry_data = call_data[
                                        (call_data['diff']==call_data['diff'].min())       
                                    ].reset_index(drop=True)    
                call_strike = call_entry_data.iloc[0]['StrikePrice']
                call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                
                if pd.isna(call_strike):
                    reason = f"Call Strike found is Null {call_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                call_exit_data = bhav_df2[
                                    (bhav_df2['StrikePrice']==call_strike)
                                    & (bhav_df2['Instrument']=="OPT"+suffix)
                                    & (bhav_df2['OptionType']=="CE")
                                ]
                
                if call_exit_data.empty:
                    reason = "Call Exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                spot_net = round(exitSpot - entrySpot, 2)        
                fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                total_net = fut_net + put_net + call_net
                total_net_with_spot = spot_net + put_net + call_net
                
                analysis_data.append({
                    "Expiry" : curr_expiry,
                    "Entry Date" : fromDate,
                    "Exit Date" : toDate,
                    
                    "Entry Spot" : entrySpot,
                    "Exit Spot" : exitSpot,
                    "Spot P&L" : spot_net,
                    
                    "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                    "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                    "Future P&L": fut_net,

                    "Put Strike" : put_strike,
                    "Put Turnover" : put_turnover_val,
                    "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                    "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                    'Put P&L' : put_net,
                    
                    "Call Strike" : call_strike,
                    "Call Turnover" : call_turnover_val,
                    "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                    "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                    "Call P&L" : call_net,

                    "Total P&L (With Future)" : total_net,
                    "Total P&L(With Spot)" : total_net_with_spot
                })
        
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', "NIFTYNXT50"]:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Tilted_{pct}Pct_Put", symbol)
                    else:    
                        path = os.path.join("Output", f"IDX_Weekly_Tilted_{pct}Pct_Put", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Tilted_{pct}Pct_Put", symbol)
                    else:    
                        path = os.path.join("Output", f"IDX_Monthly_Tilted_{pct}Pct_Put", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Tilted_{pct}Pct", symbol)
                else:
                    path = os.path.join("Output", f"STK_Monthly_Tilted_{pct}Pct_Put", symbol)


            os.makedirs(path, exist_ok=True)    
            fileName =  f"{symbol}_summary"
            
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            
            fileName = fileName +f"_Tilted_{pct}Pct_Put"
            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")


        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Tilted_{pct}Pct_Put", symbol)
                    else:    
                        path = os.path.join("Output", f"IDX_Weekly_Tilted_{pct}Pct_Put", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Tilted_{pct}Pct_Put", symbol)
                    else:    
                        path = os.path.join("Output", f"IDX_Monthly_Tilted_{pct}Pct_Put", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Tilted_{pct}Pct_Put", symbol)
                else:
                    path = os.path.join("Output", f"STK_Monthly_Tilted_{pct}Pct_Put", symbol)

            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary"
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            
            fileName = fileName +f"_Tilted__{pct}Pct_Put_Log"

            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()



# Chooses Call ITM instead of ATM - Normal with Rollover
def analyse_data_V2():  
    params_df = process_params()
    
    # Iterate through params file
    for p in range(0, len(params_df)):
        analysis_data = []
        suffix = "STK"
        row = params_df.iloc[p]

        startDate = row['FromDate']
        endDate = row['ToDate']
        symbol = row['Ticker']
        expiryBasis = row['ExpiryBasis']
        weeklyCond = not pd.isna(row['Weekly'])
        pctChgCond = not pd.isna(row['PctChg'])
        pctParam = row['PctChg']
        
        liquidCond = True
        if symbol in ["NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "NIFTYNXT50"]:
            suffix = "IDX"
        else:
            weeklyCond = False


        # Get Strike Data for symbol in params file
        df = getStrikeData(symbol)
        if df.empty:
            reason = f"Data not found for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue

        df = df[
                (df['Date']>=startDate)
                & (df['Date']<=endDate)
            ].sort_values(by='Date').reset_index(drop=True)
    
        if df.empty:
            reason = f"Data not found from {startDate} to {endDate} for symbol:{symbol}"
            createLogFile(symbol, reason, None, None, None, None)
            continue
    
        
        # Get Expiry on the expiryBasis column; Monthly
        fut_expiry_df = pd.DataFrame()

        if not weeklyCond:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        else:
            expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}.csv")
            fut_expiry_df = pd.read_csv(f"./expiryData/{expiryBasis.upper()}_Monthly.csv")
        
        expiry_df["Current Expiry"] = pd.to_datetime(expiry_df["Current Expiry"], format='%Y-%m-%d')
        expiry_df["Previous Expiry"] = pd.to_datetime(expiry_df["Previous Expiry"], format='%Y-%m-%d')
        expiry_df["Next Expiry"] = pd.to_datetime(expiry_df["Next Expiry"], format='%Y-%m-%d')
    
        expiry_df = expiry_df[
                                (expiry_df['Previous Expiry']>=df['Date'].min())
                                & (expiry_df['Current Expiry']<=df['Date'].max())
                            ].sort_values(by='Current Expiry').reset_index(drop=True)

        
        if(len(fut_expiry_df)>0):
            fut_expiry_df["Current Expiry"] = pd.to_datetime(fut_expiry_df["Current Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Previous Expiry"] = pd.to_datetime(fut_expiry_df["Previous Expiry"], format='%Y-%m-%d')
            fut_expiry_df["Next Expiry"] = pd.to_datetime(fut_expiry_df["Next Expiry"], format='%Y-%m-%d')
            fut_expiry_df = fut_expiry_df.sort_values(by='Current Expiry').reset_index(drop=True)
       
        # Iterate through expiry file
        for e in range(0, len(expiry_df)):
            expiry_row = expiry_df.iloc[e]
            prev_expiry = expiry_row["Previous Expiry"]
            curr_expiry = expiry_row["Current Expiry"]
            next_expiry = expiry_row["Next Expiry"]
            

            if pd.isna(prev_expiry) or pd.isna(curr_expiry):
                reason = f"Prev Expiry is Null" if pd.isna(prev_expiry) else f"Curr Expiry is Null"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue


            # Filter strike Data for Spot value and Percentage Change Condition
            filtered_data = df[
                                (df['Date']>=prev_expiry)
                                & (df['Date']<=curr_expiry)
                            ].sort_values(by='Date').reset_index(drop=True)
            
            if filtered_data.empty:
                reason = "No Data found between Prev and Curr Expiry"
                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                continue
                           

            # Check for Percentage Change Condition
            intervals = []
            interval_df = pd.DataFrame()

            if pctChgCond:
                filtered_data1 = filtered_data.copy(deep=True)
                filtered_data1['ReEntry'] = False 
                filtered_data1['Entry_Price'] = None
                filtered_data1['Pct_Change'] = None
                entryPrice = None
                
                for t in range(0, len(filtered_data1)):
                    if t==0:
                        entryPrice = filtered_data1.iloc[t]['Close']
                        filtered_data1.at[t, 'Entry_Price'] = entryPrice
                    else:
                        if not pd.isna(entryPrice):
                            roc = 100*((filtered_data1.iloc[t]['Close'] - entryPrice)/entryPrice)
                            filtered_data1.at[t, 'Entry_Price'] = entryPrice
                            filtered_data1.at[t, 'Pct_Change'] = round(roc, 2)
                            
                            try:
                                pctParam = float(pctParam)
                            except:
                                reason = "Error encountered in formatting PctChg Column in params.csv"
                                createLogFile(symbol, reason, prev_expiry, curr_expiry, None, None)
                                sys.exit()

                            if abs(roc)>=pctParam:
                                filtered_data1.at[t, 'ReEntry'] = True
                                entryPrice = filtered_data1.iloc[t]['Close']
                    
                filtered_data1 = filtered_data1[filtered_data1['ReEntry']==True]
                reentry_dates = []

                if(len(filtered_data1)>0):
                    reentry_dates = [
                        d for d in filtered_data1['Date']
                        if prev_expiry < d < curr_expiry
                    ]

                    start = prev_expiry
                    for d in reentry_dates:
                        intervals.append((start, d))
                        start = d   

                    intervals.append((start, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
                else:
                    intervals.append((prev_expiry, curr_expiry))
                    interval_df = pd.DataFrame(intervals, columns=['From', 'To'])

            else:
                intervals.append((prev_expiry, curr_expiry))
                interval_df = pd.DataFrame(intervals, columns=['From', 'To'])
          
            
            
            print(f"(Normal SF with ITM Call) Symbol:{symbol} PrevExpiry:{prev_expiry.strftime('%d-%m-%Y')} CurrExpiry:{curr_expiry.strftime('%d-%m-%Y')}")
            
            # Iterate through Interval dataframe created 
            for i in range(0, len(interval_df)):
                fileName1 = fileName2 = ""
                fromDate = interval_df.iloc[i]['From']
                toDate = interval_df.iloc[i]['To']
                
                if pctChgCond:
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')} PctChg:{pctParam}")
                else:    
                    print(f"From:{fromDate.strftime('%d-%m-%Y')} To:{toDate.strftime('%d-%m-%Y')}")
                
                fileName1 = fromDate.strftime("%Y-%m-%d") + ".csv"
                fileName2 = toDate.strftime("%Y-%m-%d") + ".csv"
                
                bhav_df1  = pd.DataFrame()
                bhav_df2 = pd.DataFrame()
                call_turnover_val, put_turnover_val = None, None
                call_strike, put_strike = None, None
                call_net, put_net, fut_net = None, None, None
                total_net = None

                # First Check Entry Bhavcopy and if it is, format it 
                try:
                    bhav_df1 = pd.read_csv(f"./cleaned_csvs/{fileName1}")
                except:
                    reason = f"{fileName1} not found in cleaned_csvs. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue

                bhav_df1['Date'] = pd.to_datetime(bhav_df1['Date'], format='%Y-%m-%d')
                bhav_df1['ExpiryDate'] = pd.to_datetime(bhav_df1['ExpiryDate'], format='%Y-%m-%d')
                                
                if weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    fut_expiry = fut_expiry_df[
                                    (fut_expiry_df['Current Expiry']>=curr_expiry)
                                ].sort_values(by='Current Expiry').reset_index(drop=True)
                    
                    if fut_expiry.empty:
                        reason = f"Fut Expiry not found in {expiryBasis}_Monthly.csv above or on {curr_expiry}"
                        createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                        continue

                    fut_expiry = fut_expiry.iloc[0]['Current Expiry']
                    bhav_df1_Fut = bhav_df1_Fut[
                                        (bhav_df1_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df1_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df1_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df1_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)

                
                bhav_df1 = bhav_df1[
                                (
                                    (bhav_df1['ExpiryDate']==curr_expiry)
                                    | (bhav_df1['ExpiryDate']==curr_expiry + timedelta(days=1))
                                    | (bhav_df1['ExpiryDate']==curr_expiry - timedelta(days=1))
                                )
                                & (bhav_df1['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df1_Fut = bhav_df1.copy(deep=True)
                    bhav_df1_Fut = bhav_df1_Fut[bhav_df1_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                
                if bhav_df1.empty or bhav_df1_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName1}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue


                # Check Exit Bhavcopy and if it is, format it 
                try:
                    bhav_df2 = pd.read_csv(f"./cleaned_csvs/{fileName2}")
                except:
                    reason = f"{fileName2} not found in bhavcopy. Skipping the Trade"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                    

                bhav_df2['Date'] = pd.to_datetime(bhav_df2['Date'], format='%Y-%m-%d')
                bhav_df2['ExpiryDate'] = pd.to_datetime(bhav_df2['ExpiryDate'], format='%Y-%m-%d')
                
                if weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    bhav_df2_Fut = bhav_df2_Fut[
                                        (bhav_df2_Fut['ExpiryDate'].dt.month==fut_expiry.month)
                                        & (bhav_df2_Fut['ExpiryDate'].dt.year==fut_expiry.year)
                                        & (bhav_df2_Fut['Instrument']=="FUT"+suffix)
                                        & (bhav_df2_Fut['Symbol']==symbol)
                                    ].reset_index(drop=True)
                    
        
                bhav_df2 = bhav_df2[
                                (
                                    (bhav_df2['ExpiryDate']==curr_expiry)
                                    | (bhav_df2['ExpiryDate']==curr_expiry-timedelta(days=1))
                                    | (bhav_df2['ExpiryDate']==curr_expiry + timedelta(days=1))
                                )
                                & (bhav_df2['Symbol']==symbol)
                            ].reset_index(drop=True)
                
                if not weeklyCond:
                    bhav_df2_Fut = bhav_df2.copy(deep=True)
                    bhav_df2_Fut = bhav_df2_Fut[bhav_df2_Fut['Instrument']=="FUT"+suffix].reset_index(drop=True)

                if bhav_df2.empty or bhav_df2_Fut.empty:
                    reason = f"Data for current expiry not found in {fileName2}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                

                # Now Filter the file using from and to date from intervals_df
                furtherFilter = filtered_data[
                                        (filtered_data['Date']>=fromDate)
                                        & (filtered_data['Date']<=toDate)
                                ].sort_values(by='Date').reset_index(drop=True).copy(deep=True)
                          
                
                # Get Spot for from and to date
                entrySpot = furtherFilter.iloc[0]['Close']
                exitSpot = furtherFilter.iloc[-1]['Close']
                

                put_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="PE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                call_data = bhav_df1[
                                (bhav_df1['Instrument']=="OPT"+suffix)
                                & (bhav_df1['OptionType']=="CE")
                            ].sort_values(by='StrikePrice', ascending=True).reset_index(drop=True)
                
                if put_data.empty or call_data.empty:
                    reason = f"No put data found." if put_data.empty else f"No Call data found."
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue                    

                put_data['diff'] = abs(put_data['StrikePrice'] - entrySpot)
                call_data['diff'] = abs(call_data['StrikePrice'] - entrySpot)
                put_target_strike = put_data[put_data['diff']==put_data['diff'].min()].iloc[0]['StrikePrice']    
                call_target_strike = call_data[call_data['diff']==call_data['diff'].min()].iloc[0]['StrikePrice']
                call_target_strike = call_data[call_data['StrikePrice']<call_target_strike]['StrikePrice'].max()
                
                put_entry_data = bhav_df1[
                    (bhav_df1['StrikePrice']==put_target_strike)
                    & (bhav_df1['Instrument']=="OPT"+suffix)
                    & (bhav_df1['OptionType']=="PE")
                ]
                put_exit_data = bhav_df2[
                    (bhav_df2['StrikePrice']==put_target_strike)
                    & (bhav_df2['Instrument']=="OPT"+suffix)
                    & (bhav_df2['OptionType']=="PE")
                ]

                call_entry_data = bhav_df1[
                    (bhav_df1['StrikePrice']==call_target_strike)
                    & (bhav_df1['Instrument']=="OPT"+suffix)
                    & (bhav_df1['OptionType']=="CE")
                ]
                call_exit_data = bhav_df2[
                    (bhav_df2['StrikePrice']==call_target_strike)
                    & (bhav_df2['Instrument']=="OPT"+suffix)
                    & (bhav_df2['OptionType']=="CE")
                ]
                
                fut_entry_data = bhav_df1_Fut.copy(deep=True)
                fut_exit_data = bhav_df2_Fut.copy(deep=True)
        
                if put_entry_data.empty or put_exit_data.empty:
                    reason = "Put Entry Data not found" if put_entry_data.empty else f"Put Exit Data not found for strike:{put_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue     
                elif fut_entry_data.empty or fut_exit_data.empty:
                    reason = "FUT Entry Data not found" if fut_entry_data.empty else f"FUT Exit Data not found"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                elif call_entry_data.empty or call_exit_data.empty:
                    reason = "Call Entry Data not found" if call_entry_data.empty else f"Call Exit Data not found for strike:{call_target_strike}"
                    createLogFile(symbol, reason, prev_expiry, curr_expiry, fromDate, toDate)
                    continue
                
                call_turnover_val = call_entry_data.iloc[0]['TurnOver']
                put_turnover_val = put_entry_data.iloc[0]['TurnOver']
                spot_net = round(exitSpot - entrySpot, 2)        
                fut_net = round(fut_exit_data.iloc[0]['Close']- fut_entry_data.iloc[0]['Close'], 2)
                put_net = round(put_exit_data.iloc[0]['Close'] - put_entry_data.iloc[0]['Close'], 2)
                call_net = round(call_entry_data.iloc[0]['Close'] - call_exit_data.iloc[0]['Close'], 2)
                total_net = fut_net + put_net + call_net
                total_net_with_spot = spot_net + put_net + call_net
            
                analysis_data.append({
                    "Expiry" : curr_expiry,
                    "Entry Date" : fromDate,
                    "Exit Date" : toDate,
                    
                    "Entry Spot" : entrySpot,
                    "Exit Spot" : exitSpot,
                    "Spot P&L" : spot_net,
                    
                    "Future EntryPrice": fut_entry_data.iloc[0]['Close'],
                    "Future ExitPrice" : fut_exit_data.iloc[0]['Close'],
                    "Future P&L": fut_net,

                    "Put Strike" : put_target_strike,
                    "Put Turnover" : put_turnover_val,
                    "Put EntryPrice" : put_entry_data.iloc[0]['Close'],
                    "Put ExitPrice" : put_exit_data.iloc[0]['Close'],
                    'Put P&L' : put_net,
                    
                    "Call Strike" : call_target_strike,
                    "Call Turnover" : call_turnover_val,
                    "Call EntryPrice" : call_entry_data.iloc[0]['Close'],
                    "Call ExitPrice" : call_exit_data.iloc[0]['Close'],
                    "Call P&L" : call_net,

                    "Total P&L (With Future)" : total_net,
                    "Total P&L(With Spot)" : total_net_with_spot
                })
    
        
        if analysis_data:
            analyse_df = pd.DataFrame(analysis_data)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Normal_With_ITM_Call", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Weekly_Normal_With_ITM_Call", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Normal_With_ITM_Call", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Monthly_Normal_With_ITM_Call", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Normal_With_ITM_Call", symbol)
                else:
                    path = os.path.join("Output", "STK_Monthly_Normal_With_ITM_Call", symbol)

            os.makedirs(path, exist_ok=True)    
            fileName =  f"{symbol}_summary"
            
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal_with_ITM_Call"

            analyse_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")

        
        if logFile:
            log_df = pd.DataFrame(logFile)
            path = "./Output"
            
            if symbol in ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']:    
                if weeklyCond:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Weekly_{pctParam}_Pct_Chg_Normal", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Weekly_Normal", symbol)
                else:
                    if pctChgCond:
                        path = os.path.join("Output", f"IDX_Monthly_{pctParam}_Pct_Chg_Normal", symbol)
                    else:    
                        path = os.path.join("Output", "IDX_Monthly_Normal", symbol)
            else:
                if pctChgCond:
                    path = os.path.join("Output", f"STK_Monthly_{pctParam}_Pct_Chg_Normal", symbol)
                else:
                    path = os.path.join("Output", "STK_Monthly_Normal", symbol)

            os.makedirs(path, exist_ok=True)  
            
            fileName =  f"{symbol}_summary"
            if weeklyCond:
                fileName = fileName + "_weekly"
            else:
                fileName = fileName + "_monthly"

            if pctChgCond:
                fileName = fileName + f"_{pctParam}_Pct_Chg"
            fileName = fileName +"_Normal_with_ITM_Call_Log"

            log_df.to_csv(f"{path}/{fileName}.csv", index=False)
            print(f"{fileName} saved to {path}")
            logFile.clear()


# analyse_data_with_rollover_V2(0.8)

analyse_data_V3()