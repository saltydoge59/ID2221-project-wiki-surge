"""The edit-rate line chart: palette, direct-label placement and the Altair spec (no Streamlit, so it is testable)."""
import altair as alt

# Categorical slots 1..8 of the validated palette, checked with the dataviz validator on a white surface (adjacent
# pairs, lines): every gate passes; aqua, yellow and magenta fall under 3:1 contrast, which the legend, the direct
# labels and the table view make up for. Each wiki owns one slot (its rank by archive volume), so filtering never
# repaints a line.
SLOTS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
INK, MUTED, GRID, AXIS, SURFACE = "#0b0b0b", "#898781", "#e1e0d9", "#c3c2b7", "#ffffff"
MAX_LABELS = 4    # direct labels only up to four series; the legend carries identity beyond that
LABEL_GAP = 0.07  # minimum gap between two end labels, as a fraction of the y-axis range (12 px text on a 210 px plot)


def end_labels(wide):
    """Label each line at its last point, skipping a label that would sit on the previous one."""
    last = wide.iloc[-1].sort_values(ascending=False)
    keep, floor = [], None
    gap = max(wide.to_numpy().max(), 1) * LABEL_GAP  # a fraction of the y-axis range, which is what sets the pixel gap
    for wiki, v in last.items():
        if floor is None or floor - v >= gap:
            keep.append(wiki)
            floor = v
    return keep[:MAX_LABELS]


def rate_chart(wide, colors):
    long = wide.reset_index().melt("minute", var_name="wiki", value_name="edits")
    wikis = list(wide.columns)
    color = alt.Color("wiki:N", scale=alt.Scale(domain=wikis, range=[colors[w] for w in wikis]),
                      legend=alt.Legend(title=None, orient="top", labelColor=INK, symbolType="stroke", symbolStrokeWidth=3))
    x = alt.X("minute:T", title=None, axis=alt.Axis(format="%H:%M", grid=False, labelColor=MUTED, domainColor=AXIS, tickColor=AXIS))
    y = alt.Y("edits:Q", title="edits per minute", axis=alt.Axis(gridColor=GRID, gridWidth=1, domain=False, ticks=False,
                                                                 labelColor=MUTED, titleColor=MUTED), scale=alt.Scale(zero=True))
    line = alt.Chart(long).mark_line(strokeWidth=2, strokeJoin="round", strokeCap="round").encode(x=x, y=y, color=color)

    near = alt.selection_point(nearest=True, on="pointerover", fields=["minute"], empty=False)
    tips = [alt.Tooltip("minute:T", format="%H:%M", title="minute")] + [alt.Tooltip("%s:Q" % w, title=w) for w in wikis]
    wide_rows = wide.reset_index()
    hover = alt.Chart(wide_rows).mark_rule(color=AXIS, strokeWidth=1).encode(
        x="minute:T", opacity=alt.condition(near, alt.value(1), alt.value(0)), tooltip=tips).add_params(near)
    dots = alt.Chart(long).mark_point(filled=True, size=70, stroke=SURFACE, strokeWidth=2).encode(
        x=x, y=y, color=color, opacity=alt.condition(near, alt.value(1), alt.value(0)))

    layers = [line, hover, dots]
    if len(wikis) <= MAX_LABELS:
        names = end_labels(wide)
        ends = long[(long.minute == long.minute.max()) & long.wiki.isin(names)]
        layers.append(alt.Chart(ends).mark_point(filled=True, size=70, stroke=SURFACE, strokeWidth=2).encode(x=x, y=y, color=color))
        layers.append(alt.Chart(ends).mark_text(align="left", dx=9, fontSize=12, color=INK).encode(x="minute:T", y="edits:Q", text="wiki:N"))
    return alt.layer(*layers).properties(height=320, padding={"right": 90, "left": 5, "top": 5, "bottom": 5}) \
        .configure_view(stroke=None).configure(background=SURFACE)
