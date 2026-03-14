"""
Trading Performance Spreadsheet Generator
=========================================
Converts a basic trade log into a comprehensive trading performance spreadsheet.
"""

import pandas as pd
import openpyxl
from openpyxl import load_workbook
from openpyxl.worksheet.formula import ArrayFormula
from datetime import datetime
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_TRADE_LOG = "trades.xlsx"
TEMPLATE_FILE = "user_input_files/#Tradesheet Template Long & Short. - Copy.xlsx"
OUTPUT_FILE = "output/Trading_Performance.xlsx"
OUTPUT_SHEET_NAME = "5-2,2"
STARTING_CAPITAL = 100

IMPACT_COST = 0.003
LONG_ROLLOVER_COST = 0.005
SHORT_ROLLOVER_COST = -0.005

# =============================================================================
# FUNCTIONS
# =============================================================================

def read_trade_log(file_path):
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    else:
        xl = pd.ExcelFile(file_path)
        sheet_name = xl.sheet_names[0]
        for name in xl.sheet_names:
            if 'trade' in name.lower() or 'log' in name.lower():
                sheet_name = name
                break
        df = pd.read_excel(file_path, sheet_name=sheet_name)

    column_mapping = {}
    for col in df.columns:
        col_lower = col.lower().strip()
        if 'symbol' in col_lower:
            column_mapping[col] = 'Symbol'
        elif 'trade' in col_lower and 'date' not in col_lower:
            column_mapping[col] = 'Trade'
        elif 'en.' in col_lower and 'date' in col_lower:
            column_mapping[col] = 'Entry Date'
        elif 'en.' in col_lower and 'price' in col_lower:
            column_mapping[col] = 'Entry Price'
        elif 'ex.' in col_lower and 'date' in col_lower:
            column_mapping[col] = 'Exit Date'
        elif 'ex.' in col_lower and 'price' in col_lower:
            column_mapping[col] = 'Exit Price'

    df = df.rename(columns=column_mapping)

    for col in ['Entry Date', 'Exit Date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'Trade' in df.columns:
        df['Trade'] = df['Trade'].astype(str).str.strip().str.title()

    return df


def copy_all_index_explore_sheet(output_wb, template_file):
    try:
        template_wb = load_workbook(template_file)
        if 'All Index Explore' not in template_wb.sheetnames:
            print("Warning: 'All Index Explore' sheet not found in template")
            return

        template_ws = template_wb['All Index Explore']

        if 'All Index Explore' in output_wb.sheetnames:
            del output_wb['All Index Explore']

        new_ws = output_wb.create_sheet('All Index Explore')

        for row in template_ws.iter_rows():
            for cell in row:
                new_cell = new_ws[cell.coordinate]
                new_cell.value = cell.value
                if cell.has_style:
                    new_cell.font = cell.font.copy()
                    new_cell.border = cell.border.copy()
                    new_cell.fill = cell.fill.copy()
                    new_cell.number_format = cell.number_format
                    new_cell.protection = cell.protection.copy()
                    new_cell.alignment = cell.alignment.copy()

        for col in template_ws.column_dimensions:
            new_ws.column_dimensions[col].width = template_ws.column_dimensions[col].width
        for row in template_ws.row_dimensions:
            new_ws.row_dimensions[row].height = template_ws.row_dimensions[row].height

        print("Successfully copied 'All Index Explore' sheet")
    except Exception as e:
        print(f"Warning: Could not copy 'All Index Explore' sheet: {e}")


def create_trading_performance_spreadsheet(trades_df, output_file, template_file):
    wb = openpyxl.Workbook()
    if 'Sheet' in wb.sheetnames:
        del wb['Sheet']

    copy_all_index_explore_sheet(wb, template_file)

    ws = wb.create_sheet(OUTPUT_SHEET_NAME)

    # Column widths
    column_widths = {
        'A': 15, 'B': 10, 'C': 15, 'D': 12, 'E': 15, 'F': 12,
        'G': 12, 'H': 10, 'I': 12, 'J': 15, 'K': 12, 'L': 12,
        'M': 12, 'N': 12, 'O': 12, 'P': 3, 'Q': 12, 'R': 12,
        'S': 12, 'T': 12, 'U': 10, 'V': 10, 'W': 12, 'X': 3,
        'Y': 3, 'Z': 10, 'AA': 15
    }
    for col, width in column_widths.items():
        ws.column_dimensions[col].width = width

    # =========================================================================
    # ROW 1: Constants and header date formulas
    # =========================================================================
    ws['A1'] = 'Overall'
    ws['C1'] = IMPACT_COST
    ws['D1'] = LONG_ROLLOVER_COST
    ws['E1'] = SHORT_ROLLOVER_COST
    ws['H1'] = datetime(2011, 1, 1)  # Start date filter - user can modify
    ws['I1'] = '=YEAR(H1)'
    ws['J1'] = '=INDEX(C21:C1048576,MATCH(I1,V21:V1048575,0),1)'
    ws['L1'] = '=MAX(E21:E1048576)'

    ws['C1'].number_format = '0.000'
    ws['D1'].number_format = '0.000'
    ws['E1'].number_format = '0.000'
    ws['H1'].number_format = 'YYYY-MM-DD'

    # =========================================================================
    # ROW 2: Header labels
    # =========================================================================
    ws['B2'] = 'Gross %'
    ws['C2'] = 'Impact Cost'
    ws['D2'] = 'Rollover Cost'
    ws['E2'] = 'Net %'
    ws['F2'] = 'Days'
    ws['I2'] = 'Gross %'
    ws['J2'] = 'Impact Cost'
    ws['K2'] = 'Rollover Cost'
    ws['L2'] = 'Net %'
    ws['M2'] = 'Days'

    # =========================================================================
    # ROW 3: Total Profit % - Overall (B-F) and Date-filtered (I-M with array formulas)
    # =========================================================================
    ws['A3'] = 'Total Profit %'
    ws['B3'] = '=SUM(G21:G1048576)'
    ws['C3'] = '=SUM(I21:I1048576)'
    ws['D3'] = '=SUM(J21:J1048576)'
    ws['E3'] = '=SUM(K21:K1048576)'
    ws['F3'] = '=SUM(H21:H1048576)'
    ws['H3'] = 'Total Profit %'
    ws['I3'] = ArrayFormula('I3', '=SUM(IF(H$1<=C21:C1048576,G21:G1048576,""))')
    ws['J3'] = ArrayFormula('J3', '=SUM(IF(H$1<=C21:C1048576,I21:I1048576,""))')
    ws['K3'] = ArrayFormula('K3', '=SUM(IF(H$1<=C21:C1048576,J21:J1048576,""))')
    ws['L3'] = ArrayFormula('L3', '=SUM(IF(H$1<=C21:C1048576,K21:K1048576,""))')
    ws['M3'] = ArrayFormula('M3', '=SUM(IF(H$1<=C21:C1048576,H21:H1048576,""))')
    ws['O3'] = 'Dates'
    ws['P3'] = 'Start Date'
    ws['Q3'] = 'End Date'
    ws['R3'] = 'Trades'
    ws['S3'] = 'Total Gross Profit'
    ws['T3'] = 'Average Gross Profit'
    ws['U3'] = 'Gross Month. Thk.'
    ws['V3'] = 'Total Net Profit'
    ws['W3'] = 'Average Net Profit'
    ws['X3'] = 'Net Month. Thk.'
    ws['Y3'] = 'Average Days Held'
    ws['Z3'] = 'Months Held'

    # =========================================================================
    # ROW 4: Avg % + Period: From 2008 to 2025
    # =========================================================================
    ws['A4'] = 'Avg %'
    ws['B4'] = '=AVERAGE(G21:G1048576)'
    ws['E4'] = '=AVERAGE(K21:K1048576)'
    ws['F4'] = '=AVERAGE(H21:H1048576)'
    ws['H4'] = 'Avg %'
    ws['I4'] = ArrayFormula('I4', '=AVERAGE(IF(H$1<=C21:C1048576,G21:G1048576,""))')
    ws['L4'] = ArrayFormula('L4', '=AVERAGE(IF(H$1<=C21:C1048576,K21:K1048576,""))')
    ws['M4'] = ArrayFormula('M4', '=AVERAGE(IF(H$1<=C21:C1048576,H21:H1048576,""))')
    ws['O4'] = 'From 2008 To 2025'
    ws['P4'] = datetime(2007, 12, 31)
    ws['Q4'] = datetime(2025, 7, 31)
    ws['R4'] = ArrayFormula('R4', '=COUNT(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['S4'] = ArrayFormula('S4', '=SUM(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['T4'] = ArrayFormula('T4', '=AVERAGE(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['U4'] = '=T4/Z4'
    ws['V4'] = ArrayFormula('V4', '=SUM(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['W4'] = ArrayFormula('W4', '=AVERAGE(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['X4'] = '=W4/Z4'
    ws['Y4'] = ArrayFormula('Y4', '=AVERAGE(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$H$21:$H$1048576,""))')
    ws['Z4'] = '=Y4/30.5'
    ws['P4'].number_format = 'YYYY-MM-DD'
    ws['Q4'].number_format = 'YYYY-MM-DD'

    # =========================================================================
    # ROW 5: Total Trades + Period: From 2008 to 2010
    # =========================================================================
    ws['A5'] = 'Total Trades'
    ws['B5'] = '=COUNT(G21:G1048576)'
    ws['E5'] = '=COUNT(K21:K1048576)'
    ws['F5'] = '=F4/30.5'
    ws['H5'] = 'Total Trades'
    ws['I5'] = ArrayFormula('I5', '=COUNT(IF(H$1<=C21:C1048576,G21:G1048576,""))')
    ws['L5'] = ArrayFormula('L5', '=COUNT(IF(H$1<=C21:C1048576,K21:K1048576,""))')
    ws['M5'] = '=M4/30.5'
    ws['O5'] = 'From 2008 to 2010'
    ws['P5'] = datetime(2007, 12, 31)
    ws['Q5'] = datetime(2010, 12, 31)
    ws['R5'] = ArrayFormula('R5', '=COUNT(IF((P5<=$C$21:$C$1048576)*(Q5>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['S5'] = ArrayFormula('S5', '=SUM(IF((P5<=$C$21:$C$1048576)*(Q5>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['T5'] = ArrayFormula('T5', '=AVERAGE(IF((P5<=$C$21:$C$1048576)*(Q5>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['U5'] = '=T5/Z5'
    ws['V5'] = ArrayFormula('V5', '=SUM(IF((P5<=$C$21:$C$1048576)*(Q5>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['W5'] = ArrayFormula('W5', '=AVERAGE(IF((P5<=$C$21:$C$1048576)*(Q5>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['X5'] = '=W5/Z5'
    ws['Y5'] = ArrayFormula('Y5', '=AVERAGE(IF((P5<=$C$21:$C$1048576)*(Q5>=$E$21:$E$1048576),$H$21:$H$1048576,""))')
    ws['Z5'] = '=Y5/30.5'
    ws['P5'].number_format = 'YYYY-MM-DD'
    ws['Q5'].number_format = 'YYYY-MM-DD'

    # =========================================================================
    # ROW 6: Win Ratio + Period: From 2011 to 2019
    # =========================================================================
    ws['A6'] = 'Win Ratio'
    ws['B6'] = '=COUNTIF(G21:G1048576,">0")/B5'
    ws['E6'] = '=COUNTIF(K21:K1048576,">0")/E5'
    ws['H6'] = 'Win Ratio'
    ws['I6'] = ArrayFormula('I6', '=COUNT(IF((H$1<=C21:C1048576)*(0<G21:G1048576),G21:G1048576,""))/I5')
    ws['L6'] = ArrayFormula('L6', '=COUNT(IF((H$1<=C21:C1048576)*(0<K21:K1048576),K21:K1048576,""))/L5')
    ws['O6'] = 'From 2011 to 2019'
    ws['P6'] = datetime(2010, 12, 31)
    ws['Q6'] = datetime(2019, 12, 31)
    ws['R6'] = ArrayFormula('R6', '=COUNT(IF((P6<=$C$21:$C$1048576)*(Q6>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['S6'] = ArrayFormula('S6', '=SUM(IF((P6<=$C$21:$C$1048576)*(Q6>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['T6'] = ArrayFormula('T6', '=AVERAGE(IF((P6<=$C$21:$C$1048576)*(Q6>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['U6'] = '=T6/Z6'
    ws['V6'] = ArrayFormula('V6', '=SUM(IF((P6<=$C$21:$C$1048576)*(Q6>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['W6'] = ArrayFormula('W6', '=AVERAGE(IF((P6<=$C$21:$C$1048576)*(Q6>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['X6'] = '=W6/Z6'
    ws['Y6'] = ArrayFormula('Y6', '=AVERAGE(IF((P6<=$C$21:$C$1048576)*(Q6>=$E$21:$E$1048576),$H$21:$H$1048576,""))')
    ws['Z6'] = '=Y6/30.5'
    ws['P6'].number_format = 'YYYY-MM-DD'
    ws['Q6'].number_format = 'YYYY-MM-DD'

    # =========================================================================
    # ROW 7: Win Avg + Period: From 2011 to 2025
    # =========================================================================
    ws['A7'] = 'Win Avg'
    ws['B7'] = '=AVERAGEIF(G21:G1048576,">0",G21:G1048576)'
    ws['E7'] = '=AVERAGEIF(K21:K1048576,">0",K21:K1048576)'
    ws['H7'] = 'Win Avg'
    ws['I7'] = ArrayFormula('I7', '=AVERAGE(IF((H$1<=C21:C1048576)*(0<G21:G1048576),G21:G1048576,""))')
    ws['L7'] = ArrayFormula('L7', '=AVERAGE(IF((H$1<=C21:C1048576)*(0<K21:K1048576),K21:K1048576,""))')
    ws['O7'] = 'From 2011 to 2025'
    ws['P7'] = datetime(2010, 12, 31)
    ws['Q7'] = datetime(2025, 7, 31)
    ws['R7'] = ArrayFormula('R7', '=COUNT(IF((P7<=$C$21:$C$1048576)*(Q7>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['S7'] = ArrayFormula('S7', '=SUM(IF((P7<=$C$21:$C$1048576)*(Q7>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['T7'] = ArrayFormula('T7', '=AVERAGE(IF((P7<=$C$21:$C$1048576)*(Q7>=$E$21:$E$1048576),$G$21:$G$1048576,""))')
    ws['U7'] = '=T7/Z7'
    ws['V7'] = ArrayFormula('V7', '=SUM(IF((P7<=$C$21:$C$1048576)*(Q7>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['W7'] = ArrayFormula('W7', '=AVERAGE(IF((P7<=$C$21:$C$1048576)*(Q7>=$E$21:$E$1048576),$K$21:$K$1048576,""))')
    ws['X7'] = '=W7/Z7'
    ws['Y7'] = ArrayFormula('Y7', '=AVERAGE(IF((P7<=$C$21:$C$1048576)*(Q7>=$E$21:$E$1048576),$H$21:$H$1048576,""))')
    ws['Z7'] = '=Y7/30.5'
    ws['P7'].number_format = 'YYYY-MM-DD'
    ws['Q7'].number_format = 'YYYY-MM-DD'

    # =========================================================================
    # ROW 8: Loss Avg
    # =========================================================================
    ws['A8'] = 'Loss Avg'
    ws['B8'] = '=AVERAGEIF(G21:G1048576,"<0",G21:G1048576)'
    ws['E8'] = '=AVERAGEIF(K21:K1048576,"<0",K21:K1048576)'
    ws['H8'] = 'Loss Avg'
    ws['I8'] = ArrayFormula('I8', '=AVERAGE(IF((H$1<=C21:C1048576)*(0>G21:G1048576),G21:G1048576,""))')
    ws['L8'] = ArrayFormula('L8', '=AVERAGE(IF((H$1<=C21:C1048576)*(0>K21:K1048576),K21:K1048576,""))')

    # =========================================================================
    # ROW 9: Max Winner
    # =========================================================================
    ws['A9'] = 'Max Winner'
    ws['B9'] = '=MAX(G21:G1048576)'
    ws['E9'] = '=MAX(K21:K501)'
    ws['H9'] = 'Max Winner'
    ws['I9'] = ArrayFormula('I9', '=MAX(IF(H$1<=C21:C1048576,G21:G1048576,""))')
    ws['L9'] = ArrayFormula('L9', '=MAX(IF(H$1<=C21:C1048576,K21:K1048576,""))')
    ws['O9'] = 'Rules:'

    # =========================================================================
    # ROW 10: Max Loser
    # =========================================================================
    ws['A10'] = 'Max Looser'
    ws['B10'] = '=MIN(G21:G1048576)'
    ws['E10'] = '=MIN(K21:K501)'
    ws['H10'] = 'Max Looser'
    ws['I10'] = ArrayFormula('I10', '=MIN(IF(H$1<=C21:C1048576,G21:G1048576,""))')
    ws['L10'] = ArrayFormula('L10', '=MIN(IF(H$1<=C21:C1048576,K21:K1048576,""))')

    # =========================================================================
    # ROW 11: Expectancy Ratio
    # =========================================================================
    ws['A11'] = 'Expectancy Ratio'
    ws['B11'] = '=((B6*B7)-((1-B6)*ABS(B8)))/(ABS(B8))'
    ws['E11'] = '=((E6*E7)-((1-E6)*ABS(E8)))/(ABS(E8))'
    ws['H11'] = 'Expectancy Ratio'
    ws['I11'] = '=((I6*I7)-((1-I6)*ABS(I8)))/(ABS(I8))'
    ws['L11'] = '=((L6*L7)-((1-L6)*ABS(L8)))/(ABS(L8))'

    # =========================================================================
    # ROW 12: Monthly Thickness — FIXED: uses F4 for avg days
    # =========================================================================
    ws['A12'] = 'Monthly Thickness'
    ws['B12'] = '=B4/F4*30'   # FIXED (was =B4/30*30)
    ws['E12'] = '=E4/F4*30'   # FIXED (was missing)
    ws['H12'] = 'Monthly Thickness'
    ws['I12'] = '=I4/M4*30'
    ws['L12'] = '=L4/M4*30'

    # =========================================================================
    # ROW 13: Risk Reward
    # =========================================================================
    ws['A13'] = 'Risk Reward'
    ws['B13'] = '=ABS(B7/B8)'
    ws['E13'] = '=ABS(E7/E8)'
    ws['H13'] = 'Risk Reward'
    ws['I13'] = '=ABS(I7/I8)'
    ws['L13'] = '=ABS(L7/L8)'

    # =========================================================================
    # ROW 14: CAGR — FIXED: I14/L14 use division (not @ operator)
    # =========================================================================
    ws['A14'] = 'CAGR'
    ws['B14'] = '=(INDEX($L$20:$L$1048576,MATCH(MAX($E$20:$E$1048576),$E$20:$E$1048576,0),1)/100)^(1/(((MAX(E20:E1048573)-MIN(C20:C1048576))/365)))-1'
    ws['E14'] = '=(INDEX($Q$20:$Q$1048576,MATCH(MAX($E$20:$E$1048576),$E$20:$E$1048576,0),1)/100)^(1/(((MAX(E20:E1048576)-MIN(C20:C1048576))/365)))-1'
    ws['H14'] = 'CAGR'
    ws['I14'] = '=(INDEX(L20:L1048573,MATCH(L1,E20:E1048573,0),1)/INDEX(L20:L1048573,MATCH(J1,C20:C1048573,0)-1,1))^(1/(((MAX(E20:E1048573)-J1)/365)))-1'
    ws['L14'] = '=(INDEX(Q20:Q1048573,MATCH(L1,E20:E1048573,0),1)/INDEX(Q20:Q1048573,MATCH(J1,C20:C1048573,0)-1,1))^(1/(((MAX(E20:E1048573)-J1)/365)))-1'

    # =========================================================================
    # ROW 15: DD (Drawdown)
    # =========================================================================
    ws['A15'] = 'DD'
    ws['B15'] = '=MIN($O$20:$O$1048576)'
    ws['E15'] = '=MIN($T$20:$T$1048576)'
    ws['H15'] = 'DD'
    ws['I15'] = ArrayFormula('I15', '=MIN(IF(H$1<=C21:C1048576,O21:O1048576,""))')
    ws['L15'] = ArrayFormula('L15', '=MIN(IF(J$1<=C21:C1048576,T20:T1048575,""))')

    # =========================================================================
    # ROW 16: CAR/MDD
    # =========================================================================
    ws['A16'] = 'CAR/MDD'
    ws['B16'] = '=ABS(B14/B15)'
    ws['E16'] = '=ABS(E14/E15)'
    ws['H16'] = 'CAR/MDD'
    ws['I16'] = '=ABS(I14/I15)'
    ws['L16'] = '=ABS(L14/L15)'

    # =========================================================================
    # ROW 17: Max MAE — FIXED: L17 is array formula, E17 is simple
    # =========================================================================
    ws['A17'] = 'Max MAE'
    ws['E17'] = '=MIN($W$21:$W$374)'
    ws['H17'] = 'Max MAE'
    ws['L17'] = ArrayFormula('L17', '=MIN(IF(H1<=$E$21:$E$374,$W$21:$W$374,""))')

    # =========================================================================
    # ROW 18: Avg MAE — FIXED: L18 is array formula, E18 is simple
    # =========================================================================
    ws['A18'] = 'Avg MAE'
    ws['E18'] = '=AVERAGE($W$21:$W$374)'   # FIXED (was missing, now correct)
    ws['H18'] = 'Avg MAE'
    ws['L18'] = ArrayFormula('L18', '=AVERAGE(IF(H1<=$E$21:$E$374,$W$21:$W$374,""))')
    ws['M18'] = 'Gross'
    ws['R18'] = 'Net'

    # =========================================================================
    # ROW 19: Sub-headers for equity tracking columns
    # =========================================================================
    ws['L19'] = 'CUMM'
    ws['M19'] = 'Peak'
    ws['N19'] = 'DD'
    ws['O19'] = '%DD'
    ws['Q19'] = 'CUMM'
    ws['R19'] = 'Peak'
    ws['S19'] = 'DD'
    ws['T19'] = '%DD'
    ws['U19'] = 'Year by exit Date'
    ws['V19'] = 'Year by entry Date'

    # =========================================================================
    # ROW 20: Data Headers + Starting equity
    # =========================================================================
    ws['A20'] = 'Symbol'
    ws['B20'] = 'Trade'
    ws['C20'] = 'En. date'
    ws['D20'] = 'En. Price'
    ws['E20'] = 'Ex. date'
    ws['F20'] = 'Ex. Price'
    ws['G20'] = '% chg'
    ws['H20'] = 'Days'
    ws['I20'] = 'Impact cost'
    ws['J20'] = 'Rollover Cost'
    ws['K20'] = 'Net Profit'
    ws['L20'] = STARTING_CAPITAL
    ws['M20'] = '=MAX(L20,M19)'
    ws['N20'] = '=IF(M20>L20,L20-M20,"")'
    ws['Q20'] = STARTING_CAPITAL
    ws['R20'] = '=MAX(Q20,R19)'
    ws['S20'] = '=IF(R20>Q20,Q20-R20,"")'
    ws['W20'] = 'MAE'
    ws['Z20'] = 'Year'
    ws['AA20'] = 'Net Return'

    # =========================================================================
    # DATA ROWS starting at row 21
    # =========================================================================
    for idx, row in trades_df.iterrows():
        r = idx + 21

        ws[f'A{r}'] = row.get('Symbol', '')
        ws[f'B{r}'] = row.get('Trade', '')

        entry_date = row.get('Entry Date')
        if pd.notna(entry_date):
            ws[f'C{r}'] = pd.to_datetime(entry_date)
            ws[f'C{r}'].number_format = 'YYYY-MM-DD'

        ws[f'D{r}'] = row.get('Entry Price', 0)

        exit_date = row.get('Exit Date')
        if pd.notna(exit_date):
            ws[f'E{r}'] = pd.to_datetime(exit_date)
            ws[f'E{r}'].number_format = 'YYYY-MM-DD'

        ws[f'F{r}'] = row.get('Exit Price', 0)

        # % Change
        ws[f'G{r}'] = f'=IF(B{r}="Long",(F{r}/D{r}-1),-(F{r}/D{r}-1))'
        ws[f'G{r}'].number_format = '0.000%'

        # Days in trade
        ws[f'H{r}'] = f'=E{r}-C{r}'

        # Impact Cost
        ws[f'I{r}'] = '=C$1'
        ws[f'I{r}'].number_format = '0.000%'

        # Rollover Cost
        ws[f'J{r}'] = f'=IF(B{r}="Long",H{r}/30*D$1,H{r}/30*E$1)'
        ws[f'J{r}'].number_format = '0.000%'

        # Net Profit
        ws[f'K{r}'] = f'=G{r}-SUM(I{r},J{r})'
        ws[f'K{r}'].number_format = '0.000%'

        # Gross Equity Tracking
        ws[f'L{r}'] = f'=L{r-1}*(1+G{r})'
        ws[f'M{r}'] = f'=MAX(L{r},M{r-1})'
        ws[f'N{r}'] = f'=IF(M{r}>L{r},L{r}-M{r},"")'
        ws[f'O{r}'] = f'=IFERROR(N{r}/M{r},0)'
        ws[f'O{r}'].number_format = '0.000%'

        # Net Equity Tracking
        ws[f'Q{r}'] = f'=Q{r-1}*(1+K{r})'
        ws[f'R{r}'] = f'=MAX(Q{r},R{r-1})'
        ws[f'S{r}'] = f'=IF(R{r}>Q{r},Q{r}-R{r},"")'
        ws[f'T{r}'] = f'=IFERROR(S{r}/R{r},0)'
        ws[f'T{r}'].number_format = '0.000%'

        # Year columns
        ws[f'U{r}'] = f'=YEAR(E{r})'
        ws[f'V{r}'] = f'=YEAR(C{r})'

        # MAE (Maximum Adverse Excursion) - Array formula
        mae_formula = (
            f'=IF(B{r}="Long",'
            f'MIN(IF('
            f"(A{r}='All Index Explore'!$A$2:$A$50000)*"
            f"(C{r}<'All Index Explore'!$B$2:$B$50000)*"
            f"(E{r}>='All Index Explore'!$B$2:$B$50000),"
            f"'All Index Explore'!$E$2:$E$50000,"
            f'""))/D{r}-1,'
            f'-(MAX(IF('
            f"(A{r}='All Index Explore'!$A$2:$A$50000)*"
            f"(C{r}<'All Index Explore'!$B$2:$B$50000)*"
            f"(E{r}>='All Index Explore'!$B$2:$B$50000),"
            f"'All Index Explore'!$D$2:$D$50000,"
            f'""))/D{r}-1))'
        )
        ws[f'W{r}'] = ArrayFormula(f'W{r}', mae_formula)
        ws[f'W{r}'].number_format = '0.000%'

        # Year (exit year)
        ws[f'Z{r}'] = f'=U{r}'

        # Yearly Net Return
        last_row = 20 + len(trades_df)
        ws[f'AA{r}'] = (
            f'=IFERROR(IF(SUMIF($U$21:$U${last_row},Z{r},$K$21:$K${last_row})=0,'
            f'NA(),'
            f'SUMIF($U$21:$U${last_row},Z{r},$K$21:$K${last_row})),'
            f'"")'
        )

        # Price formatting
        for col in ['D', 'F']:
            ws[f'{col}{r}'].number_format = '0.00'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    wb.save(output_file)
    print(f"Saved: {output_file}")
    return output_file


def main():
    print("TRADING PERFORMANCE SPREADSHEET GENERATOR")

    if not os.path.exists(INPUT_TRADE_LOG):
        print(f"Input file not found: {INPUT_TRADE_LOG}")
        print("Creating sample trade log for demonstration...")
        sample_trades = pd.DataFrame({
            'Symbol': ['NIFTYBANK', 'NIFTYBANK', 'NIFTY', 'NIFTY', 'NIFTYBANK'],
            'Trade': ['Short', 'Long', 'Long', 'Short', 'Long'],
            'En. date': ['2008-01-31', '2010-12-31', '2011-03-15', '2011-06-01', '2011-09-10'],
            'En. Price': [9226.25, 11791.45, 5832.30, 8900.50, 9800.75],
            'Ex. date': ['2008-11-21', '2011-09-26', '2011-05-20', '2011-08-15', '2011-11-25'],
            'Ex. Price': [4300.70, 9384.50, 6200.10, 8500.25, 10200.50]
        })
        sample_trades.to_excel(INPUT_TRADE_LOG, index=False)

    try:
        trades_df = read_trade_log(INPUT_TRADE_LOG)
        create_trading_performance_spreadsheet(trades_df, OUTPUT_FILE, TEMPLATE_FILE)
        print(f"Done! Total trades: {len(trades_df)}")
    except Exception as e:
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
