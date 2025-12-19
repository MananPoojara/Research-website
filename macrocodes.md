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
Sub FormatTargetTable(tbl As Table)

    Dim totalWidth As Single
    Dim i As Integer
    Dim otherCols As Integer
    Dim r As Row, c As Cell

    On Error Resume Next

    ' ============================
    ' EXACT RULER POSITIONING
    ' ============================
    tbl.AllowAutoFit = False
    tbl.Rows.Alignment = wdAlignRowLeft

    tbl.Rows.LeftIndent = CentimetersToPoints(0.3)

    totalWidth = CentimetersToPoints(14.6)
    tbl.PreferredWidthType = wdPreferredWidthPoints
    tbl.PreferredWidth = totalWidth

    ' ============================
    ' COLUMN WIDTHS
    ' ============================
    tbl.Columns(1).Width = CentimetersToPoints(6)

    otherCols = tbl.Columns.Count - 1
    For i = 2 To tbl.Columns.Count
        tbl.Columns(i).Width = (totalWidth - tbl.Columns(1).Width) / otherCols
    Next i

    ' ============================
    ' ROW LABELS
    ' ============================
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

    ' ============================
    ' BOLD RULES
    ' ============================
    tbl.Range.Font.Bold = False
    BoldRow tbl, 1
    BoldRow tbl, 3
    BoldRow tbl, 4
    BoldRow tbl, 7
    BoldRow tbl, 8

    ' ============================
    ' REMOVE ALL CELL BORDERS
    ' ============================
    For Each r In tbl.Rows
        For Each c In r.Cells
            c.Borders.Enable = False
        Next c
    Next r

    ' ============================
    ' OUTSIDE BORDER
    ' ============================
    With tbl.Borders
        .OutsideLineStyle = wdLineStyleSingle
        .OutsideLineWidth = wdLineWidth100pt
        .OutsideColor = wdColorBlack
    End With

    ' ============================
    ' COLUMN 1 RIGHT BORDER
    ' ============================
    For Each c In tbl.Columns(1).Cells
        With c.Borders(wdBorderRight)
            .LineStyle = wdLineStyleSingle
            .LineWidth = wdLineWidth100pt
            .Color = wdColorBlack
        End With
    Next c

    ' ============================
    ' HEADER BOTTOM BORDER (ROW 1)
    ' ============================
    For Each c In tbl.Rows(1).Cells
        With c.Borders(wdBorderBottom)
            .LineStyle = wdLineStyleSingle
            .LineWidth = wdLineWidth100pt
            .Color = wdColorBlack
        End With
    Next c

    On Error GoTo 0

End Sub ' =============================
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

