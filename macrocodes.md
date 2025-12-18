Sub FormatAllStrategyPerformanceTables()

    Dim tbl As Table
    Dim foundAny As Boolean
    foundAny = False

    For Each tbl In ActiveDocument.Tables

        ' Identify Strategy Performance tables
        If InStr(1, CleanCellText(tbl.Cell(2, 1).Range.Text), _
                  "Total Gross Profit", vbTextCompare) > 0 Then
            FormatTargetTable tbl
            foundAny = True
        End If

    Next tbl

    If Not foundAny Then
        MsgBox "No Strategy Performance tables found.", vbExclamation
    Else
        MsgBox "Strategy Performance tables formatted successfully.", vbInformation
    End If

End Sub


' =============================
' FORMAT ONE TABLE
' =============================
Sub FormatTargetTable(tbl As Table)

    Dim lastRow As Long
    Dim r As Long, c As Cell

    On Error Resume Next

    ' ---- COLUMN WIDTH FIX ----
    tbl.AllowAutoFit = False
    tbl.Columns(1).Width = CentimetersToPoints(6)

    ' ---- RENAME ROW LABELS ----
    SetLabel tbl, 1, "Period"
    SetLabel tbl, 2, "Total Gross Profit %"
    SetLabel tbl, 3, "Total Net Profit %"
    SetLabel tbl, 4, "Net Avg. Profit/Loss %"
    SetLabel tbl, 5, "Avg. Days Held"
    SetLabel tbl, 6, "Total Trades"
    SetLabel tbl, 7, "Net Win %"
    SetLabel tbl, 8, "Net Monthly Thickness %"
    SetLabel tbl, 9, "CAGR %"
    SetLabel tbl, 10, "Max Drawdown %"
    SetLabel tbl, 11, "CAR/MDD"

    ' ---- CLEAR ALL BOLD FIRST ----
    tbl.Range.Font.Bold = False

    ' ---- APPLY WHOLE-ROW BOLD ----
    BoldRow tbl, 1     ' Period header
    BoldRow tbl, 3     ' Total Net Profit %
    BoldRow tbl, 4     ' Net Avg. Profit/Loss %
    BoldRow tbl, 7     ' Net Win %
    BoldRow tbl, 8     ' Net Monthly Thickness %

    ' ---- FIX BOTTOM BORDER ----
    lastRow = tbl.Rows.Count
    For Each c In tbl.Rows(lastRow).Cells
        With c.Borders(wdBorderBottom)
            .LineStyle = wdLineStyleSingle
            .LineWidth = wdLineWidth150pt
            .Color = wdColorBlack
        End With
    Next c

    ' ---- ENSURE OUTSIDE BORDERS ----
    tbl.Borders.OutsideLineStyle = wdLineStyleSingle
    tbl.Borders.OutsideLineWidth = wdLineWidth150pt
    tbl.Borders.OutsideColor = wdColorBlack

    On Error GoTo 0

End Sub


' =============================
' SET LABEL TEXT
' =============================
Sub SetLabel(tbl As Table, rowNum As Long, txt As String)
    With tbl.Cell(rowNum, 1).Range
        .Text = txt
        .ParagraphFormat.Alignment = wdAlignParagraphLeft
        .ParagraphFormat.SpaceAfter = 0
    End With
End Sub


' =============================
' BOLD ENTIRE ROW
' =============================
Sub BoldRow(tbl As Table, rowNum As Long)
    tbl.Rows(rowNum).Range.Font.Bold = True
End Sub


' =============================
' CLEAN CELL TEXT
' =============================
Function CleanCellText(txt As String) As String
    CleanCellText = Replace(txt, Chr(13), "")
    CleanCellText = Replace(CleanCellText, Chr(7), "")
    CleanCellText = Trim(CleanCellText)
End Function
