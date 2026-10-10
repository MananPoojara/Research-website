
Attaching ChatGPT conversation.md file. Here it is [MD file conversation] You have to continue the ChatGPT pending work first of all, that pending, the book converts, and after that I need some of the books that are not in share drive, and if it is in share drive, then take from that research drive. If there is no books available for the below authors, then use AgentReach skill and go to the browser or other MCp servers for LibGen or SciFi or another site. Connect them and download the books and convert it into strategy, author by author, for below authors, after you've done the pending work. And I need it to be objective, rule-based books that can be converted to strategy and show the best tested data and all. Don't make mistake like Joe Rose, and also from the research share, every other author after the authors that I need first, you can continue the work for the other authors. You have to run long horizon using sub-agents, some of the plugins and skills. If you need the permission, ask for it first, so you can start doing the work. And add it your bash commands and other commands in your config. Ask me for the permission right now, so you can start doing the work and go ahead.

1. Nick Radge

2. Michael Covel

3. Cesar Alvarez

4. Greg Capra
5. Van tharp



libgen.li
sci-hub.ren
archive.org
annas-archive.gl





# Research-website

Arbitrage claude chat: https://claude.ai/share/6f22ee40-1452-4148-911a-341a1aae4401
newwwww
https://drive.google.com/file/d/12sohmW-wPiBrG-ux-SCIJWh2kEjIOcXS/view?usp=sharing



https://drive.google.com/file/d/1GyWK9dJy4RhxddwqQ7aumO5i65zR6jld/view?usp=drive_link

https://drive.google.com/file/d/1EBNaUQksQT58ejv70pFhdr51-kSc6vt-/view?usp=sharing


https://manan-ai-quant-portfolio.mananpujarawork.chatgpt.site

https://gsrtc-easy.mananpujarawork.chatgpt.site/

sudo apt update && sudo apt upgrade -y && sudo apt install -y ca-certificates curl gnupg git ufw docker.io docker-compose && sudo ufw allow OpenSSH && sudo ufw allow 80 && sudo ufw allow 443 && sudo ufw --force enable && sudo systemctl enable --now docker && sudo usermod -aG docker $USER && mkdir -p ~/youngturtle-server/infra/nginx ~/youngturtle-server/infra/compose ~/youngturtle-server/infra/volumes/{postgres,redis,minio} && touch ~/youngturtle-server/infra/docker-compose.yml ~/youngturtle-server/infra/nginx/default.conf


GenerateMultipleFiles.py

import os
import glob
import sys
import pandas as pd
import numpy as np
import math
import csv
from datetime import datetime, timedelta
import re
import shutil
from time import sleep


columnLogic = {
    'Ticker': 'first',
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last',
    'Volume': 'sum',
    'OpenInterest': 'last',
    'Weekday': 'first'
}


symbolMondayWeeklyData = pd.DataFrame()
symbolExpiryWeeklyData = pd.DataFrame()
symbolMonthlyData = pd.DataFrame()
symbolYearlyData = pd.DataFrame()


def getYearlyReturns(row):
    global symbolYearlyData
    yearlyReturns = [np.nan, np.nan]
    yearlyRow = symbolYearlyData[symbolYearlyData['Date'] == datetime(row['Date'].year, 1, 1)]
    if (len(yearlyRow) > 0):
        yearlyReturnPoints = yearlyRow['ReturnPoints'].iloc[0]
        yearlyReturnPercentage = yearlyRow['ReturnPercentage'].iloc[0]
        yearlyReturns[0] = yearlyReturnPoints
        yearlyReturns[1] = yearlyReturnPercentage
    return yearlyReturns


def getMonthlyReturns(row):
    global symbolMonthlyData
    monthlyReturns = [np.nan, np.nan]
    monthlyRow = symbolMonthlyData[symbolMonthlyData['Date'] == datetime(row['Date'].year, row['Date'].month, 1)]
    if (len(monthlyRow) > 0):
        monthlyReturnPoints = monthlyRow['ReturnPoints'].iloc[0]
        monthlyReturnPercentage = monthlyRow['ReturnPercentage'].iloc[0]
        monthlyReturns[0] = monthlyReturnPoints
        monthlyReturns[1] = monthlyReturnPercentage
    return monthlyReturns


def getMondayWeeklyData(row):
    global symbolMondayWeeklyData
    mondayWeeklyData = [np.nan, np.nan, np.nan, np.nan]
    mondayRow = symbolMondayWeeklyData[symbolMondayWeeklyData['Date'] == row['MondayWeeklyDate']]
    if (len(mondayRow) > 0):
        mondayWeeklyReturnPoints = mondayRow['ReturnPoints'].iloc[0]
        mondayWeeklyReturnPercentage = mondayRow['ReturnPercentage'].iloc[0]
        mondayWeekNumberMonthly = mondayRow['WeekNumberMonthly'].iloc[0]
        mondayWeekNumberYearly = mondayRow['WeekNumberYearly'].iloc[0]
        mondayWeeklyData[0] = mondayWeeklyReturnPoints
        mondayWeeklyData[1] = mondayWeeklyReturnPercentage
        mondayWeeklyData[2] = mondayWeekNumberMonthly
        mondayWeeklyData[3] = mondayWeekNumberYearly
    return mondayWeeklyData


def getExpiryWeeklyData(row):
    global symbolExpiryWeeklyData
    expiryWeeklyData = [np.nan, np.nan, np.nan, np.nan]
    expiryRow = symbolExpiryWeeklyData[symbolExpiryWeeklyData['Date'] == row['ExpiryWeeklyDate']]
    if (len(expiryRow) > 0):
        expiryWeeklyReturnPoints = expiryRow['ReturnPoints'].iloc[0]
        expiryWeeklyReturnPercentage = expiryRow['ReturnPercentage'].iloc[0]
        expiryWeekNumberMonthly = expiryRow['WeekNumberMonthly'].iloc[0]
        expiryWeekNumberYearly = expiryRow['WeekNumberYearly'].iloc[0]
        expiryWeeklyData[0] = expiryWeeklyReturnPoints
        expiryWeeklyData[1] = expiryWeeklyReturnPercentage
        expiryWeeklyData[2] = expiryWeekNumberMonthly
        expiryWeeklyData[3] = expiryWeekNumberYearly
    return expiryWeeklyData


def safe_boolean(value):
    """Convert value to boolean, preserving NaN"""
    if pd.isna(value):
        return np.nan
    return bool(value > 0)


def safe_even_check(value):
    """Check if value is even, preserving NaN"""
    if pd.isna(value):
        return np.nan
    return bool((value % 2) == 0)


def generateFiles(symbolDailyData, fileStorePath):

    global symbolMondayWeeklyData
    global symbolExpiryWeeklyData
    global symbolMonthlyData
    global symbolYearlyData

    # Read Daily data file and format columns
    print('Daily data file and format columns')
    symbolDailyData['Date'] = pd.to_datetime(symbolDailyData['Date'], format='%d-%m-%Y')
    symbolDailyData['Weekday'] = symbolDailyData['Date'].dt.day_name()
    symbolDailyData = symbolDailyData.set_index('Date').sort_index(ascending=True)
    print('Done')

    # Convert daily to weekly timeframe using groupby instead of resample
    print('Convert daily to weekly timeframe from monday as start of week')
    
    # Calculate Monday date for each record
    symbolDailyData_temp = symbolDailyData.reset_index()
    symbolDailyData_temp['MondayDate'] = symbolDailyData_temp['Date'] - pd.to_timedelta(symbolDailyData_temp['Date'].dt.weekday, unit='D')
    
    # Group by Monday date and aggregate
    symbolMondayWeeklyData = symbolDailyData_temp.groupby('MondayDate').agg({
        'Ticker': 'first',
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OpenInterest': 'last',
        'Weekday': 'first'
    }).reset_index()
    
    symbolMondayWeeklyData.rename(columns={'MondayDate': 'Date'}, inplace=True)
    symbolMondayWeeklyData['Weekday'] = symbolMondayWeeklyData['Date'].dt.day_name()
    symbolMondayWeeklyData = symbolMondayWeeklyData.set_index('Date').sort_index(ascending=True)
    print('Done')

    # Convert daily to weekly timeframe (Expiry week - Thursday) using groupby
    print('Convert daily to weekly timeframe from expiry week and thursday as end of week')
    
    symbolDailyData_temp = symbolDailyData.reset_index()
    
    # Calculate Thursday date for each record (end of expiry week)
    def get_thursday_of_week(date):
        weekday = date.weekday()  # 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday, 4=Friday
        if weekday <= 3:  # Monday to Thursday
            days_to_thursday = 3 - weekday
        else:  # Friday, Saturday, Sunday
            days_to_thursday = 3 + (7 - weekday)
        return date + pd.Timedelta(days=days_to_thursday)
    
    symbolDailyData_temp['ThursdayDate'] = symbolDailyData_temp['Date'].apply(get_thursday_of_week)
    
    # Group by Thursday date and aggregate
    symbolExpiryWeeklyData = symbolDailyData_temp.groupby('ThursdayDate').agg({
        'Ticker': 'first',
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum',
        'OpenInterest': 'last',
        'Weekday': 'first'
    }).reset_index()
    
    symbolExpiryWeeklyData.rename(columns={'ThursdayDate': 'Date'}, inplace=True)
    symbolExpiryWeeklyData['StartDate'] = symbolExpiryWeeklyData['Date'] - pd.Timedelta(days=6)
    symbolExpiryWeeklyData['Weekday'] = symbolExpiryWeeklyData['Date'].dt.day_name()
    symbolExpiryWeeklyData = symbolExpiryWeeklyData.set_index('Date').sort_index(ascending=True)
    print('Done')

    # Convert daily to monthly timeframe
    print('Convert daily to monthly timeframe')
    symbolMonthlyData = symbolDailyData.resample('M').apply(columnLogic).reset_index()
    symbolMonthlyData['Date'] = pd.to_datetime(symbolMonthlyData['Date'].dt.strftime('%m-%Y'))
    symbolMonthlyData['Weekday'] = symbolMonthlyData['Date'].dt.day_name()
    symbolMonthlyData = symbolMonthlyData.set_index('Date').sort_index(ascending=True)
    print('Done')

    # Convert daily to yearly timeframe
    print('Convert daily to yearly timeframe')
    symbolYearlyData = symbolDailyData.resample('Y').apply(columnLogic).reset_index()
    symbolYearlyData['Date'] = pd.to_datetime(symbolYearlyData['Date'].dt.strftime('%Y'))
    symbolYearlyData['Weekday'] = symbolYearlyData['Date'].dt.day_name()
    symbolYearlyData = symbolYearlyData.set_index('Date').sort_index(ascending=True)
    print('Done')

    # Reset all dataframe indices
    symbolDailyData.reset_index(inplace=True)
    symbolMondayWeeklyData.reset_index(inplace=True)
    symbolExpiryWeeklyData.reset_index(inplace=True)
    symbolMonthlyData.reset_index(inplace=True)
    symbolYearlyData.reset_index(inplace=True)

    # generate new fields of yearly data
    print('Generate new fields of yearly data')
    symbolYearlyData['EvenYear'] = symbolYearlyData['Date'].dt.year.apply(safe_even_check)
    symbolYearlyData['ReturnPoints'] = symbolYearlyData['Close'] - symbolYearlyData['Close'].shift(1)
    symbolYearlyData['ReturnPercentage'] = round((symbolYearlyData['ReturnPoints'] / symbolYearlyData['Close'].shift(1)*100), 2)
    symbolYearlyData['PositiveYear'] = symbolYearlyData['ReturnPoints'].apply(safe_boolean)
    print('Done')

    # generate new fields of monthly data
    print('Generate new fields of monthly data')
    symbolMonthlyData['EvenMonth'] = symbolMonthlyData['Date'].dt.month.apply(safe_even_check)
    symbolMonthlyData['ReturnPoints'] = symbolMonthlyData['Close'] - symbolMonthlyData['Close'].shift(1)
    symbolMonthlyData['ReturnPercentage'] = round((symbolMonthlyData['ReturnPoints'] / symbolMonthlyData['Close'].shift(1)*100), 2)
    symbolMonthlyData['PositiveMonth'] = symbolMonthlyData['ReturnPoints'].apply(safe_boolean)

    yearlyReturns = np.array(symbolMonthlyData.apply(lambda row: getYearlyReturns(row), axis=1).tolist()).transpose()
    symbolMonthlyData['EvenYear'] = symbolMonthlyData['Date'].dt.year.apply(safe_even_check)
    symbolMonthlyData['YearlyReturnPoints'], symbolMonthlyData['YearlyReturnPercentage'] = yearlyReturns[0], yearlyReturns[1]
    symbolMonthlyData['PositiveYear'] = symbolMonthlyData['YearlyReturnPoints'].apply(safe_boolean)
    print('Done')

    # generate new fields of monday weekly data
    print('Generate new fields of monday weekly data')
    symbolMondayWeeklyData['WeekNumberMonthly'] = symbolMondayWeeklyData['WeekNumberYearly'] = np.nan

    for i in range(1, len(symbolMondayWeeklyData)):
        if (symbolMondayWeeklyData.loc[i, 'Date'].month != symbolMondayWeeklyData.loc[i-1, 'Date'].month):
            symbolMondayWeeklyData.loc[i, 'WeekNumberMonthly'] = 1
        else:
            symbolMondayWeeklyData.loc[i, 'WeekNumberMonthly'] = symbolMondayWeeklyData.loc[i-1, 'WeekNumberMonthly'] + 1
        if (symbolMondayWeeklyData.loc[i, 'Date'].year != symbolMondayWeeklyData.loc[i-1, 'Date'].year):
            symbolMondayWeeklyData.loc[i, 'WeekNumberYearly'] = 1
        else:
            symbolMondayWeeklyData.loc[i, 'WeekNumberYearly'] = symbolMondayWeeklyData.loc[i-1, 'WeekNumberYearly'] + 1

    symbolMondayWeeklyData['EvenWeekNumberMonthly'] = symbolMondayWeeklyData['WeekNumberMonthly'].apply(safe_even_check)
    symbolMondayWeeklyData['EvenWeekNumberYearly'] = symbolMondayWeeklyData['WeekNumberYearly'].apply(safe_even_check)
    symbolMondayWeeklyData[['WeekNumberMonthly', 'WeekNumberYearly']] = symbolMondayWeeklyData[
        ['WeekNumberMonthly', 'WeekNumberYearly']
    ].fillna(value=0).astype('Int64').replace(0, np.nan)

    symbolMondayWeeklyData['ReturnPoints'] = symbolMondayWeeklyData['Close'] - symbolMondayWeeklyData['Close'].shift(1)
    symbolMondayWeeklyData['ReturnPercentage'] = round((symbolMondayWeeklyData['ReturnPoints'] / symbolMondayWeeklyData['Close'].shift(1)*100), 2)
    symbolMondayWeeklyData['PositiveWeek'] = symbolMondayWeeklyData['ReturnPoints'].apply(safe_boolean)

    monthlyReturns = np.array(symbolMondayWeeklyData.apply(lambda row: getMonthlyReturns(row), axis=1).tolist()).transpose()
    symbolMondayWeeklyData['EvenMonth'] = symbolMondayWeeklyData['Date'].dt.month.apply(safe_even_check)
    symbolMondayWeeklyData['MonthlyReturnPoints'], symbolMondayWeeklyData['MonthlyReturnPercentage'] = monthlyReturns[0], monthlyReturns[1]
    symbolMondayWeeklyData['PositiveMonth'] = symbolMondayWeeklyData['MonthlyReturnPoints'].apply(safe_boolean)

    yearlyReturns = np.array(symbolMondayWeeklyData.apply(lambda row: getYearlyReturns(row), axis=1).tolist()).transpose()
    symbolMondayWeeklyData['EvenYear'] = symbolMondayWeeklyData['Date'].dt.year.apply(safe_even_check)
    symbolMondayWeeklyData['YearlyReturnPoints'], symbolMondayWeeklyData['YearlyReturnPercentage'] = yearlyReturns[0], yearlyReturns[1]
    symbolMondayWeeklyData['PositiveYear'] = symbolMondayWeeklyData['YearlyReturnPoints'].apply(safe_boolean)
    print('Done')

    # generate new fields of expiry weekly data
    print('Generate new fields of expiry weekly data')
    symbolExpiryWeeklyData['WeekNumberMonthly'] = symbolExpiryWeeklyData['WeekNumberYearly'] = np.nan

    for i in range(1, len(symbolExpiryWeeklyData)):
        if (symbolExpiryWeeklyData.loc[i, 'Date'].month != symbolExpiryWeeklyData.loc[i-1, 'Date'].month):
            symbolExpiryWeeklyData.loc[i, 'WeekNumberMonthly'] = 1
        else:
            symbolExpiryWeeklyData.loc[i, 'WeekNumberMonthly'] = symbolExpiryWeeklyData.loc[i-1, 'WeekNumberMonthly'] + 1
        if (symbolExpiryWeeklyData.loc[i, 'Date'].year != symbolExpiryWeeklyData.loc[i-1, 'Date'].year):
            symbolExpiryWeeklyData.loc[i, 'WeekNumberYearly'] = 1
        else:
            symbolExpiryWeeklyData.loc[i, 'WeekNumberYearly'] = symbolExpiryWeeklyData.loc[i-1, 'WeekNumberYearly'] + 1

    symbolExpiryWeeklyData['EvenWeekNumberMonthly'] = symbolExpiryWeeklyData['WeekNumberMonthly'].apply(safe_even_check)
    symbolExpiryWeeklyData['EvenWeekNumberYearly'] = symbolExpiryWeeklyData['WeekNumberYearly'].apply(safe_even_check)
    symbolExpiryWeeklyData[['WeekNumberMonthly', 'WeekNumberYearly']] = symbolExpiryWeeklyData[
        ['WeekNumberMonthly', 'WeekNumberYearly']
    ].fillna(value=0).astype('Int64').replace(0, np.nan)

    symbolExpiryWeeklyData['ReturnPoints'] = symbolExpiryWeeklyData['Close'] - symbolExpiryWeeklyData['Close'].shift(1)
    symbolExpiryWeeklyData['ReturnPercentage'] = round((symbolExpiryWeeklyData['ReturnPoints']/symbolExpiryWeeklyData['Close'].shift(1)*100), 2)
    symbolExpiryWeeklyData['PositiveWeek'] = symbolExpiryWeeklyData['ReturnPoints'].apply(safe_boolean)

    monthlyReturns = np.array(symbolExpiryWeeklyData.apply(lambda row: getMonthlyReturns(row), axis=1).tolist()).transpose()
    symbolExpiryWeeklyData['EvenMonth'] = symbolExpiryWeeklyData['Date'].dt.month.apply(safe_even_check)
    symbolExpiryWeeklyData['MonthlyReturnPoints'], symbolExpiryWeeklyData['MonthlyReturnPercentage'] = monthlyReturns[0], monthlyReturns[1]
    symbolExpiryWeeklyData['PositiveMonth'] = symbolExpiryWeeklyData['MonthlyReturnPoints'].apply(safe_boolean)

    yearlyReturns = np.array(symbolExpiryWeeklyData.apply(lambda row: getYearlyReturns(row), axis=1).tolist()).transpose()
    symbolExpiryWeeklyData['EvenYear'] = symbolExpiryWeeklyData['Date'].dt.year.apply(safe_even_check)
    symbolExpiryWeeklyData['YearlyReturnPoints'], symbolExpiryWeeklyData['YearlyReturnPercentage'] = yearlyReturns[0], yearlyReturns[1]
    symbolExpiryWeeklyData['PositiveYear'] = symbolExpiryWeeklyData['YearlyReturnPoints'].apply(safe_boolean)
    print('Done')

    # generate new fields of daily data
    print('Generate new fields of daily data')
    symbolDailyData['CalenderMonthDay'] = symbolDailyData['Date'].dt.day
    symbolDailyData['CalenderYearDay'] = symbolDailyData['Date'].dt.dayofyear
    symbolDailyData['TradingMonthDay'] = symbolDailyData['TradingYearDay'] = np.nan

    for i in range(1, len(symbolDailyData)):
        if (symbolDailyData.loc[i, 'Date'].month != symbolDailyData.loc[i-1, 'Date'].month):
            symbolDailyData.loc[i, 'TradingMonthDay'] = 1
        else:
            symbolDailyData.loc[i, 'TradingMonthDay'] = symbolDailyData.loc[i - 1, 'TradingMonthDay'] + 1
        if (symbolDailyData.loc[i, 'Date'].year != symbolDailyData.loc[i-1, 'Date'].year):
            symbolDailyData.loc[i, 'TradingYearDay'] = 1
        else:
            symbolDailyData.loc[i, 'TradingYearDay'] = symbolDailyData.loc[i - 1, 'TradingYearDay'] + 1

    symbolDailyData['EvenCalenderMonthDay'] = symbolDailyData['CalenderMonthDay'].apply(safe_even_check)
    symbolDailyData['EvenCalenderYearDay'] = symbolDailyData['CalenderYearDay'].apply(safe_even_check)
    symbolDailyData['EvenTradingMonthDay'] = symbolDailyData['TradingMonthDay'].apply(safe_even_check)
    symbolDailyData['EvenTradingYearDay'] = symbolDailyData['TradingYearDay'].apply(safe_even_check)

    symbolDailyData[['TradingMonthDay', 'TradingYearDay']] = symbolDailyData[
        ['TradingMonthDay', 'TradingYearDay']
    ].fillna(value=0).astype('Int64').replace(0, np.nan)

    symbolDailyData['ReturnPoints'] = symbolDailyData['Close'] - symbolDailyData['Close'].shift(1)
    symbolDailyData['ReturnPercentage'] = round((symbolDailyData['ReturnPoints'] / symbolDailyData['Close'].shift(1)*100), 2)
    symbolDailyData['PositiveDay'] = symbolDailyData['ReturnPoints'].apply(safe_boolean)

    # weekly monday calculations
    symbolDailyData['MondayWeeklyDate'] = symbolDailyData['Date'].apply(lambda x: x - pd.tseries.frequencies.to_offset(str(x.weekday()) + 'D'))
    mondayWeeklyData = np.array(symbolDailyData.apply(lambda row: getMondayWeeklyData(row), axis=1).tolist()).transpose()

    symbolDailyData['MondayWeekNumberMonthly'] = symbolDailyData['MondayWeekNumberYearly'] = np.nan
    symbolDailyData['MondayWeekNumberMonthly'] = mondayWeeklyData[2]
    symbolDailyData['MondayWeekNumberYearly'] = mondayWeeklyData[3]
    symbolDailyData['EvenMondayWeekNumberMonthly'] = pd.Series(mondayWeeklyData[2]).apply(safe_even_check)
    symbolDailyData['EvenMondayWeekNumberYearly'] = pd.Series(mondayWeeklyData[3]).apply(safe_even_check)
    symbolDailyData['MondayWeeklyReturnPoints'] = mondayWeeklyData[0]
    symbolDailyData['MondayWeeklyReturnPercentage'] = mondayWeeklyData[1]
    symbolDailyData['PositiveMondayWeek'] = pd.Series(mondayWeeklyData[0]).apply(safe_boolean)

    # weekly expiry calculations
    symbolDailyData['ExpiryWeeklyDate'] = symbolDailyData['Date'].apply(
        lambda x: (x + pd.tseries.frequencies.to_offset(str(6) + 'D')) if (x.weekday() == 4)
        else (x + pd.tseries.frequencies.to_offset(str(3-x.weekday()) + 'D'))
    )
    expiryWeeklyData = np.array(symbolDailyData.apply(lambda row: getExpiryWeeklyData(row), axis=1).tolist()).transpose()

    symbolDailyData['ExpiryWeekNumberMonthly'] = symbolDailyData['ExpiryWeekNumberYearly'] = np.nan
    symbolDailyData['ExpiryWeekNumberMonthly'] = expiryWeeklyData[2]
    symbolDailyData['ExpiryWeekNumberYearly'] = expiryWeeklyData[3]
    symbolDailyData['EvenExpiryWeekNumberMonthly'] = pd.Series(expiryWeeklyData[2]).apply(safe_even_check)
    symbolDailyData['EvenExpiryWeekNumberYearly'] = pd.Series(expiryWeeklyData[3]).apply(safe_even_check)
    symbolDailyData['ExpiryWeeklyReturnPoints'] = expiryWeeklyData[0]
    symbolDailyData['ExpiryWeeklyReturnPercentage'] = expiryWeeklyData[1]
    symbolDailyData['PositiveExpiryWeek'] = pd.Series(expiryWeeklyData[0]).apply(safe_boolean)

    # replace not a number values in data frame
    symbolDailyData[['MondayWeekNumberMonthly', 'MondayWeekNumberYearly']] = symbolDailyData[
        ['MondayWeekNumberMonthly', 'MondayWeekNumberYearly']
    ].fillna(value=0).astype('Int64').replace(0, np.nan)
    symbolDailyData[['ExpiryWeekNumberMonthly', 'ExpiryWeekNumberYearly']] = symbolDailyData[
        ['ExpiryWeekNumberMonthly', 'ExpiryWeekNumberYearly']
    ].fillna(value=0).astype('Int64').replace(0, np.nan)

    # monthly data calculation
    monthlyReturns = np.array(symbolDailyData.apply(lambda row: getMonthlyReturns(row), axis=1).tolist()).transpose()
    symbolDailyData['EvenMonth'] = symbolDailyData['Date'].dt.month.apply(safe_even_check)
    symbolDailyData['MonthlyReturnPoints'], symbolDailyData['MonthlyReturnPercentage'] = monthlyReturns[0], monthlyReturns[1]
    symbolDailyData['PositiveMonth'] = symbolDailyData['MonthlyReturnPoints'].apply(safe_boolean)

    # yearly data calculation
    yearlyReturns = np.array(symbolDailyData.apply(lambda row: getYearlyReturns(row), axis=1).tolist()).transpose()
    symbolDailyData['EvenYear'] = symbolDailyData['Date'].dt.year.apply(safe_even_check)
    symbolDailyData['YearlyReturnPoints'], symbolDailyData['YearlyReturnPercentage'] = yearlyReturns[0], yearlyReturns[1]
    symbolDailyData['PositiveYear'] = symbolDailyData['YearlyReturnPoints'].apply(safe_boolean)
    print('Done')

    # save all files to csv
    print('Save all files to csv')
    symbolDailyData.set_index('Date').to_csv(fileStorePath + '1_Daily.csv')
    symbolMondayWeeklyData.set_index('Date').to_csv(fileStorePath + '2_MondayWeekly.csv')
    symbolExpiryWeeklyData.set_index('Date').to_csv(fileStorePath + '3_ExpiryWeekly.csv')
    symbolMonthlyData.set_index('Date').to_csv(fileStorePath + '4_Monthly.csv')
    symbolYearlyData.set_index('Date').to_csv(fileStorePath + '5_Yearly.csv')
    print('Done')


"""
    Main code logic starts
"""


symbolsFolderPath = './Symbols'
count = 0

if not (os.path.exists(symbolsFolderPath)):
    os.makedirs(symbolsFolderPath)

FILE_PATH = r"./others/Seasonality.csv"
print("File Path", FILE_PATH)
dataFile = pd.read_csv(FILE_PATH)
print('Total records in file : ', len(dataFile))
dataFile = dataFile.astype({
    'Ticker': str,
    'Open': np.float64,
    'High': np.float64,
    'Low': np.float64,
    'Close': np.float64,
    'Volume': np.int64,
    'OpenInterest': np.int64
})
# dataFile['Date'] = pd.to_datetime(dataFile['Date'], format='%Y-%m-%d')
dataFile['Date'] = pd.to_datetime(dataFile['Date'], format='%d-%m-%Y')
symbols_sorted_data = dataFile.groupby(['Ticker'], group_keys=False).apply(lambda x: x.sort_values(
    ['Date'], ascending=True)).reset_index(drop=True)

print('\nTotal symbols : ', len(symbols_sorted_data['Ticker'].unique()))
print('---------------------------------------------------------------------------------------------')


for ticker in (symbols_sorted_data['Ticker'].unique()):

    count = count + 1
    print(count, ticker)

    tickerData = symbols_sorted_data[symbols_sorted_data['Ticker'] == ticker]
    if ((tickerData.iloc[0]['Date'].day) < 8):
        tickerData = tickerData.iloc[(8 - (tickerData.iloc[0]['Date'].day)):]
    tickerData['Date'] = tickerData['Date'].dt.strftime('%d-%m-%Y')
    filePath = symbolsFolderPath + '//' + ticker + '//'

    if not (os.path.exists(filePath)):
        os.makedirs(filePath)

    generateFiles(tickerData, filePath)

    print('---------------------------------------------------------------------------------------------')
