We are redesigning the UI layout of the Event Analysis page.

IMPORTANT RULES — READ CAREFULLY:

DO NOT change any business logic.

DO NOT change any API calls.

DO NOT modify backend.

DO NOT change any calculation logic.

DO NOT modify TradingView Lightweight chart configuration (x-axis, y-axis, series logic).

DO NOT rename props.

DO NOT restructure data contracts.

DO NOT change filter logic.

DO NOT remove any existing functionality.

DO NOT refactor stores (Zustand/Context).

DO NOT change chart data mapping logic.

DO NOT change aggregated metrics calculations.

This task is strictly a UI layout restructuring task.

🎯 Goal

Redesign the UI layout to visually match the attached reference image.

We want:

Left vertical navigation sidebar

Main center analytics workspace

Right-side filter console

Clean white institutional theme

Professional spacing and hierarchy

Compact but elegant chart blocks

Analytics panel on the right side (distribution + metrics)

Data table below charts

All functionality must remain exactly the same.

🧱 REQUIRED STRUCTURE

Implement this layout structure:

1️⃣ Left Sidebar (Navigation Only)

Vertical menu

Icons + labels

Active state indicator

No filters here

No chart logic here

Pure navigation UI only

2️⃣ Center Main Workspace

Top:

Page header (Symbol + subtitle)

Below:

Horizontal compact stats strip (Total Events, Win Rate, Avg Return, Sharpe, Profit Factor)
Use existing stats values only.
Do NOT modify calculations.

Below:

Main Event Chart (TradingView Lightweight)
Use existing chart component.
Do NOT modify its internal logic.
Only adjust container layout/styling.

Below chart:

Two smaller panels side by side:

Cumulative Profit (existing logic)

Pattern Returns (existing logic)

Below:

Data Table (existing table component)

No logic changes.

3️⃣ Right Sidebar (Filter Console)

Move all current filters into a right sidebar panel.

This panel should include:

Symbol selector

Date range picker

Event category

Event name

Window before/after sliders

Trade configuration

Minimum occurrences

Analyze button

All existing state management must remain intact.

Only reposition and restyle.

🎨 DESIGN RULES

White theme only.

Background: pure white

Section cards: soft light background

Subtle borders only

Remove heavy shadows

Remove strong purple saturation

Use accent color only for:

Active tab

Analyze button

Event-day vertical line

Charts must remain untouched.

Spacing must be consistent (8px grid system).

🚫 WHAT NOT TO DO

Do NOT rewrite chart components.

Do NOT re-fetch data differently.

Do NOT alter useQuery keys.

Do NOT reformat API payloads.

Do NOT remove current features.

Do NOT redesign filters logic.

Do NOT invent new metrics unless using already available backend data.

If additional metrics panels are needed for layout symmetry, reuse existing data fields only.

🧠 Implementation Strategy

Only:

Move components

Adjust containers

Update flex/grid layout

Refine spacing

Improve typography

Improve visual hierarchy

No business logic changes.

If something requires logic change, stop and explain before modifying.

End of instructions.
