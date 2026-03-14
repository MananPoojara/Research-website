this the code that genrate excel sheet 5-2,2 that i am attaching 


so what's wrong here is 

below thing is missing or not correct 

Here are the formulas exactly as they appear in the 5-2,2 sheet for the range you specified. The formulas follow the exact same logic as the template sheet, summarizing the trade data (which starts at row 21).

Top Header Dates:

I1: =YEAR(H1)

J1: =INDEX(C21:C1048576,MATCH(I1,V21:V1048575,0),1)

L1: =MAX(E21:E1048576)

Total Profit % & Averages (Rows 3-4):

I3 (Gross %): {=SUM(IF(H$1<=C21:C1048576,G21:G1048576,""))}

J3 (Impact Cost): {=SUM(IF(H$1<=C21:C1048576,I21:I1048576,""))}

K3 (Rollover Cost): {=SUM(IF(H$1<=C21:C1048576,J21:J1048576,""))}

L3 (Net %): {=SUM(IF(H$1<=C21:C1048576,K21:K1048576,""))}

M3 (Days): {=SUM(IF(H$1<=C21:C1048576,H21:H1048576,""))}

I4 (Avg Gross %): {=AVERAGE(IF(H$1<=C21:C1048576,G21:G1048576,""))}

L4 (Avg Net %): {=AVERAGE(IF(H$1<=C21:C1048576,K21:K1048576,""))}

Trade Statistics (Rows 5-10):

I5 / L5 (Total Trades): {=COUNT(IF(H$1<=C21:C1048576,G21:G1048576,""))} / {=COUNT(IF(H$1<=C21:C1048576,K21:K1048576,""))}

I6 / L6 (Win Ratio): {=COUNT(IF((H$1<=C21:C1048576)*(0<G21:G1048576),G21:G1048576,""))/I5} / {=COUNT(IF((H$1<=C21:C1048576)*(0<K21:K1048576),K21:K1048576,""))/L5}

I7 / L7 (Win Avg): {=AVERAGE(IF((H$1<=C21:C1048576)*(0<G21:G1048576),G21:G1048576,""))} / {=AVERAGE(IF((H$1<=C21:C1048576)*(0<K21:K1048576),K21:K1048576,""))}

I8 / L8 (Loss Avg): {=AVERAGE(IF((H$1<=C21:C1048576)*(0>G21:G1048576),G21:G1048576,""))} / {=AVERAGE(IF((H$1<=C21:C1048576)*(0>K21:K1048576),K21:K1048576,""))}

I9 / L9 (Max Winner): {=MAX(IF(H$1<=C21:C1048576,G21:G1048576,""))} / {=MAX(IF(H$1<=C21:C1048576,K21:K1048576,""))}

I10 / L10 (Max Looser): {=MIN(IF(H$1<=C21:C1048576,G21:G1048576,""))} / {=MIN(IF(H$1<=C21:C1048576,K21:K1048576,""))}

System Metrics (Rows 11-18):

I11 / L11 (Expectancy Ratio): =((I6*I7)-((1-I6)*ABS(I8)))/(ABS(I8)) / =((L6*L7)-((1-L6)*ABS(L8)))/(ABS(L8))

I12 / L12 (Monthly Thickness): =I4/M4*30 / =L4/M4*30

I13 / L13 (Risk Reward): =ABS(I7/I8) / =ABS(L7/L8)

I14 (CAGR Gross): =(INDEX(L20:L1048573,MATCH(L1,E20:E1048573,0),1)/@INDEX(L20:L1048573,MATCH(J1,C20:C1048573,0)-1,1))^(1/(((MAX(E20:E1048573)-J1)/365)))-1

L14 (CAGR Net): =(INDEX(Q20:Q1048573,MATCH(L1,E20:E1048573,0),1)/@INDEX(Q20:Q1048573,MATCH(J1,C20:C1048573,0)-1,1))^(1/(((MAX(E20:E1048573)-J1)/365)))-1

I15 / L15 (DD): {=MIN(IF(H$1<=C21:C1048576,O21:O1048576,""))} / {=MIN(IF(J$1<=C21:C1048576,T20:T1048575,""))}

I16 / L16 (CAR/MDD): =ABS(I14/I15) / =ABS(L14/L15)

L17 (Max MAE): {=MIN(IF(H1<=$E$21:$E$374,$W$21:$W$374,""))}

L18 (Avg MAE): {=AVERAGE(IF(H1<=$E$21:$E$374,$W$21:$W$374,""))}

Period Breakdown Array Formulas (Columns R to Z, Rows 4-7): (These repeat for the different date groupings in rows 4, 5, 6, and 7)

Trades (R4): {=COUNT(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$G$21:$G$1048576,""))}

Total Gross Profit (S4): {=SUM(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$G$21:$G$1048576,""))}

Avg Gross Profit (T4): {=AVERAGE(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$G$21:$G$1048576,""))}

Gross Month. Thk (U4): =T4/Z4

Total Net Profit (V4): {=SUM(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$K$21:$K$1048576,""))}

Avg Net Profit (W4): {=AVERAGE(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$K$21:$K$1048576,""))}

Net Month. Thk (X4): =W4/Z4

Avg Days Held (Y4): {=AVERAGE(IF((P4<=$C$21:$C$1048576)*(Q4>=$E$21:$E$1048576),$H$21:$H$1048576,""))}

Months Held (Z4): =Y4/30.5




e12 : =E4/F4*30
avg MAE : =AVERAGE($W$21:$W$374) 

this things missing and wrong so update code
