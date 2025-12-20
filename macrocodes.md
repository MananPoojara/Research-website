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
        .OutsideLineWidth = wdLineWidth150pt
        .OutsideColor = wdColorBlack
    End With

    ' ============================
    ' COLUMN 1 RIGHT BORDER
    ' ============================
    Dim r1 As Long

    For r1 = 2 To tbl.Rows.Count
        With tbl.Cell(r1, 1).Borders(wdBorderRight)
            .LineStyle = wdLineStyleSingle
            .LineWidth = wdLineWidth050pt
            .Color = wdColorBlack
        End With
    Next r1

    ' ============================
    ' HEADER BOTTOM BORDER (ROW 1)
    ' ============================
    For Each c In tbl.Rows(1).Cells
        With c.Borders(wdBorderBottom)
            .LineStyle = wdLineStyleSingle
            .LineWidth = wdLineWidth050pt
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




//---------------------------------Chart Adjustment------------------------------------//
Sub FixAndCenterLinkedChartsBelowStrategyHeading()

    Dim p As Paragraph
    Dim anchorPos As Long
    Dim ils As InlineShape
    Dim shp As Shape
    Dim pageWidth As Single
    Dim foundHeading As Boolean

    foundHeading = False
    anchorPos = 0

    ' --------------------------------------------
    ' 1. Find heading position
    ' --------------------------------------------
    For Each p In ActiveDocument.Paragraphs
        If InStr(1, p.Range.Text, _
            "Strategy Performance (From 01-01-2008 to 02-12-2025):", _
            vbTextCompare) > 0 Then

            anchorPos = p.Range.End
            foundHeading = True
            Exit For
        End If
    Next p

    If Not foundHeading Then
        MsgBox "Strategy Performance heading not found.", vbExclamation
        Exit Sub
    End If

    ' Page width for centering
    pageWidth = ActiveDocument.PageSetup.PageWidth _
                - ActiveDocument.PageSetup.LeftMargin _
                - ActiveDocument.PageSetup.RightMargin

    ' --------------------------------------------
    ' 2. HANDLE INLINE LINKED CHARTS
    ' --------------------------------------------
    For Each ils In ActiveDocument.InlineShapes

        If ils.Range.Start > anchorPos Then
            If ils.Type = wdInlineShapeChart Then

                ' Convert inline → floating
                Set shp = ils.ConvertToShape

                Call FormatFloatingChart(shp, pageWidth)
            End If
        End If

    Next ils

    ' --------------------------------------------
    ' 3. HANDLE FLOATING LINKED CHARTS
    ' --------------------------------------------
    For Each shp In ActiveDocument.Shapes

        If shp.Anchor.Start > anchorPos Then
            If shp.HasChart Then
                Call FormatFloatingChart(shp, pageWidth)
            End If
        End If

    Next shp

    MsgBox "All linked charts below heading formatted correctly.", vbInformation

End Sub


' ==================================================
' FORMAT FLOATING CHART (THE KEY PART)
' ==================================================
Sub FormatFloatingChart(shp As Shape, pageWidth As Single)

    With shp
        ' Unlock layout constraints
        .LockAspectRatio = msoFalse
        .RelativeHorizontalPosition = wdRelativeHorizontalPositionMargin
        .RelativeVerticalPosition = wdRelativeVerticalPositionParagraph

        ' Disable wrapping issues
        .WrapFormat.Type = wdWrapNone

        ' Set size
        .Width = CentimetersToPoints(14.29)
        .Height = CentimetersToPoints(14.29)

        ' Center horizontally
        .Left = (pageWidth - .Width) / 2

    End With

End Sub


#------------------------------------------------------------------------------#
Sub FixTradesheetColumnWidths_AllRows()

    Dim p As Paragraph
    Dim tbl As Table
    Dim headingPos As Long
    Dim found As Boolean
    Dim widths As Variant
    Dim r As Long
    Dim c As Long
    Dim maxCols As Long

    found = False
    headingPos = -1

    ' ----------------------------------------
    ' 1. Find Tradesheet heading
    ' ----------------------------------------
    For Each p In ActiveDocument.Paragraphs
        If InStr(1, p.Range.Text, "Tradesheet:", vbTextCompare) > 0 Then
            headingPos = p.Range.End
            Exit For
        End If
    Next p

    If headingPos = -1 Then
        MsgBox "Tradesheet heading not found.", vbExclamation
        Exit Sub
    End If

    ' ----------------------------------------
    ' 2. Find first table after heading
    ' ----------------------------------------
    For Each tbl In ActiveDocument.Tables
        If tbl.Range.Start > headingPos Then
            found = True
            Exit For
        End If
    Next tbl

    If Not found Then
        MsgBox "Tradesheet table not found.", vbExclamation
        Exit Sub
    End If

    ' ----------------------------------------
    ' 3. Lock layout
    ' ----------------------------------------
    tbl.AllowAutoFit = False

    ' ----------------------------------------
    ' 4. Expected column widths (cm)
    ' ----------------------------------------
    widths = Array(2.2, 1.93, 2.3, 1.9, 1.71, 1.86, 1.51, 1.88)
    maxCols = UBound(widths) + 1

    ' ----------------------------------------
    ' 5. Normalize ALL DATA ROWS
    '    (do NOT touch header row = 1)
    ' ----------------------------------------
    For r = 2 To tbl.Rows.Count
        ' Unmerge row completely
        tbl.Rows(r).Cells.Merge

        ' Split back into required columns
        tbl.Rows(r).Cells(1).Split NumRows:=1, NumColumns:=maxCols
    Next r

    ' ----------------------------------------
    ' 6. Apply column widths (now consistent)
    ' ----------------------------------------
    For c = 1 To maxCols
        tbl.Columns(c).Width = CentimetersToPoints(widths(c - 1))
    Next c

    MsgBox "Tradesheet columns fixed for ALL rows.", vbInformation

End Sub

