Sub FormatAllStrategyPerformanceTables()

    Dim tbl As Table
    Dim cellText As String
    Dim foundAny As Boolean
    foundAny = False

    For Each tbl In ActiveDocument.Tables

        ' Identify Strategy Performance tables safely
        cellText = CleanCellText(tbl.Cell(2, 1).Range.Text)

        If InStr(1, cellText, "Total Gross Profit", vbTextCompare) > 0 Then
            Call FormatTargetTable1(tbl)
            foundAny = True
        End If

    Next tbl

    If Not foundAny Then
        MsgBox "No Strategy Performance tables found.", vbExclamation
    Else
        MsgBox "All Strategy Performance tables formatted successfully.", vbInformation
    End If

End Sub


' =============================
' FORMAT ONE TARGET TABLE
' =============================
Sub FormatTargetTable(tbl As Table)

    On Error Resume Next

    ' ---- FIX COLUMN WIDTH ISSUE ----
    tbl.AllowAutoFit = False
    tbl.Columns(1).Width = CentimetersToPoints(6)

    ' ---- RENAME ROWS + BOLD RULES ----
    SetRow tbl.Cell(1, 1), "Period", True
    SetRow tbl.Cell(2, 1), "Total Gross Profit %", False
    SetRow tbl.Cell(3, 1), "Total Net Profit %", True
    SetRow tbl.Cell(4, 1), "Net Avg. Profit/Loss %", True
    SetRow tbl.Cell(5, 1), "Avg. Days Held", False
    SetRow tbl.Cell(6, 1), "Total Trades", False
    SetRow tbl.Cell(7, 1), "Net Win %", True
    SetRow tbl.Cell(8, 1), "Net Monthly Thickness %", True
    SetRow tbl.Cell(9, 1), "CAGR %", False
    SetRow tbl.Cell(10, 1), "Max Drawdown %", False
    SetRow tbl.Cell(11, 1), "CAR/MDD", False

    On Error GoTo 0

End Sub


' =============================
' SET ROW TEXT + BOLD
' =============================
Sub SetRow(c As Cell, txt As String, isBold As Boolean)

    With c.Range
        .Text = txt
        .Font.Bold = isBold
        .ParagraphFormat.SpaceAfter = 0
        .ParagraphFormat.Alignment = wdAlignParagraphLeft
    End With

End Sub


' =============================
' CLEAN CELL TEXT
' =============================
Function CleanCellText(txt As String) As String
    CleanCellText = Replace(txt, Chr(13), "")
    CleanCellText = Replace(CleanCellText, Chr(7), "")
    CleanCellText = Trim(CleanCellText)
End Function

Sub FormatTargetTable1(tbl As Table)

    Dim lastRow As Long
    Dim c As Cell

    On Error Resume Next

    ' ---- FIX COLUMN WIDTH ----
    tbl.AllowAutoFit = False
    tbl.Columns(1).Width = CentimetersToPoints(5)

    ' ---- RENAME ROWS + BOLD RULES ----
    SetRow tbl.Cell(1, 1), "Period", True
    SetRow tbl.Cell(2, 1), "Total Gross Profit %", False
    SetRow tbl.Cell(3, 1), "Total Net Profit %", True
    SetRow tbl.Cell(4, 1), "Net Avg. Profit/Loss %", True
    SetRow tbl.Cell(5, 1), "Avg. Days Held", False
    SetRow tbl.Cell(6, 1), "Total Trades", False
    SetRow tbl.Cell(7, 1), "Net Win %", True
    SetRow tbl.Cell(8, 1), "Net Monthly Thickness %", True
    SetRow tbl.Cell(9, 1), "CAGR %", False
    SetRow tbl.Cell(10, 1), "Max Drawdown %", False
    SetRow tbl.Cell(11, 1), "CAR/MDD", False

    ' ---- FIX MISSING BOTTOM BORDER ----
    lastRow = tbl.Rows.Count

    For Each c In tbl.Rows(lastRow).Cells
        With c.borders(wdBorderBottom)
            .LineStyle = wdLineStyleSingle
            .LineWidth = wdLineWidth150pt
            .Color = wdColorBlack
        End With
    Next c

    ' ---- ENSURE OUTSIDE BORDERS ----
    tbl.borders.OutsideLineStyle = wdLineStyleSingle
    tbl.borders.OutsideLineWidth = wdLineWidth150pt
    tbl.borders.OutsideColor = wdColorBlack

    On Error GoTo 0

End Sub





#------------------------------------------------#
